"""
ChannelConsumer — ingestão MQTT estilo KEPServer (channel model): UMA conexão,
wildcard subscribe em milhares de tópicos, auto-discovery de devices/tags.

Por que existe: o modelo antigo abria 1 client MQTT POR device (N conexões + N
threads num processo Python só) — não escala para milhares de tags. Um channel
do KEPServer é o oposto: 1 conexão física : N tags. Este consumer assina
`optiflow/#` (ou `$share/<grupo>/optiflow/#` p/ HA via shared subscription
MQTT5, distribuindo a carga entre réplicas), parseia `optiflow/{device}/{tag}`,
e publica em lote no Redis stream (mesmo GatewayReading que o historian já
consome — compatível por construção).

paho roda em thread própria (loop_start); on_message só enfileira (deque é
thread-safe); uma task asyncio drena e publica em lote.
"""
from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from ..core.bus import GatewayBus
from ..core.schemas import GatewayReading

log = logging.getLogger(__name__)


def _device_type(device_id: str) -> str:
    if device_id.startswith("VRP"):
        return "VRP"
    if device_id.startswith(("CRAT", "RES")):
        return "RESERVOIR"
    return "DEVICE"


def _parse_ts(raw: object) -> datetime:
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


class ChannelConsumer:
    def __init__(
        self,
        bus: GatewayBus,
        gateway_id: str,
        host: str,
        port: int = 1883,
        topic: str = "optiflow/#",
        flush_interval_s: float = 0.5,
        max_batch: int = 1000,
    ):
        self._bus = bus
        self._gateway_id = gateway_id
        self._host, self._port, self._topic = host, port, topic
        self._flush_interval = flush_interval_s
        self._max_batch = max_batch
        self._buf: deque[GatewayReading] = deque(maxlen=200_000)
        self._client: mqtt.Client | None = None
        self.received = 0
        self.published = 0
        self.dropped = 0
        self.requeued = 0
        self._devices: set[str] = set()  # auto-discovery

    async def start(self) -> None:
        client = mqtt.Client(client_id=f"optiflow-channel-{self._gateway_id}")
        client.on_connect = self._on_connect
        client.on_message = self._on_message
        client.reconnect_delay_set(min_delay=1, max_delay=10)
        client.connect_async(self._host, self._port, keepalive=30)
        client.loop_start()
        self._client = client
        asyncio.create_task(self._flush_loop(), name="channel-flush")
        log.info("channel_consumer.started host=%s topic=%s", self._host, self._topic)

    def _on_connect(self, cl: mqtt.Client, _ud, _flags, rc: int) -> None:
        if rc == 0:
            cl.subscribe(self._topic, qos=0)  # re-subscreve em cada reconnect
            log.info("channel_consumer.subscribed topic=%s", self._topic)
        else:
            log.warning("channel_consumer.connect_failed rc=%s", rc)

    def _on_message(self, _cl, _ud, msg: mqtt.MQTTMessage) -> None:
        # tópico: optiflow/{device_id}/{tag_id}
        parts = msg.topic.split("/")
        if len(parts) < 3:
            return
        device_id, tag_id = parts[1], parts[2]
        try:
            p = json.loads(msg.payload)
            value = float(p["value"])
        except (ValueError, TypeError, KeyError):
            self.dropped += 1
            return
        reading = GatewayReading(
            ts=_parse_ts(p.get("ts")),
            gateway_id=self._gateway_id,
            device_id=device_id,
            device_type=_device_type(device_id),
            tag_id=tag_id,
            value=value,
            quality=p.get("quality", "good"),
            protocol="mqtt",
        )
        self._buf.append(reading)
        self.received += 1
        self._devices.add(device_id)

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval)
            batch: list[GatewayReading] = []
            while self._buf and len(batch) < self._max_batch:
                batch.append(self._buf.popleft())
            if batch:
                try:
                    await self._bus.publish_readings_batch(batch)
                    self.published += len(batch)
                except Exception as e:  # noqa: BLE001 — não derruba a ingestão
                    # Store-and-forward (estilo KEPServer): o sink falhou (ex.:
                    # redis em blip) -> re-enfileira o lote (NÃO perde dado) e
                    # tenta de novo. A ordem não importa: cada reading carrega
                    # seu próprio ts. Limitado por maxlen do deque.
                    self._buf.extendleft(reversed(batch))
                    self.requeued += len(batch)
                    log.error("channel_consumer.publish_error err=%s re-enfileirado=%d buffered=%d",
                              e, len(batch), len(self._buf))
                    await asyncio.sleep(1)

    def stats(self) -> dict:
        return {
            "received": self.received,
            "published": self.published,
            "dropped": self.dropped,
            "requeued": self.requeued,
            "buffered": len(self._buf),
            "devices_discovered": len(self._devices),
        }

    async def stop(self) -> None:
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()
            self._client = None
