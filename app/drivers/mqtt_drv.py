"""Driver MQTT — paho-mqtt (bridge para asyncio via Queue)."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

from .base import DriverStatus, ProtocolDriver
from ..core.registry import DeviceConfig, TagConfig

log = logging.getLogger(__name__)
_DEFAULT_PORT = 1883


def _extract(payload_str: str, json_path: str | None) -> float:
    payload_str = payload_str.strip()
    if not json_path:
        try:
            return float(payload_str)
        except ValueError:
            data = json.loads(payload_str)
            return float(data.get("value", 0))
    data = json.loads(payload_str)
    for key in json_path.split("."):
        data = data[key]
    return float(data)


class MqttDriver(ProtocolDriver):
    PROTOCOL = "mqtt"

    def __init__(self, cfg: DeviceConfig, gateway_id: str = "optiflow-gw-01"):
        super().__init__(cfg, gateway_id)
        self._client   = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._latest:  dict[str, float]   = {}
        self._ts_map:  dict[str, datetime] = {}
        self._topic_tag: dict[str, TagConfig] = {}
        host_port  = cfg.endpoint.split(":")
        self._host = host_port[0]
        self._port = int(host_port[1]) if len(host_port) > 1 else _DEFAULT_PORT

    async def connect(self) -> None:
        import paho.mqtt.client as mqtt
        self._status = DriverStatus.CONNECTING
        self._loop   = asyncio.get_running_loop()
        opts = self.cfg.options
        client = mqtt.Client(client_id=f"optiflow-{self.cfg.device_id}")
        if opts.get("username"):
            client.username_pw_set(opts["username"], opts.get("password", ""))
        self._topic_tag = {tag.address: tag for tag in self.cfg.tags}
        connected_event = asyncio.Event()

        def on_connect(cl, _ud, _flags, rc):
            if rc == 0:
                for topic in self._topic_tag:
                    cl.subscribe(topic, qos=opts.get("qos", 0))
                self._loop.call_soon_threadsafe(connected_event.set)

        def on_message(_cl, _ud, msg):
            tag = self._topic_tag.get(msg.topic)
            if not tag:
                return
            try:
                value = _extract(msg.payload.decode(), tag.options.get("json_path"))
                self._loop.call_soon_threadsafe(self._update, tag.tag_id, value)
            except Exception:
                pass

        def on_disconnect(_cl, _ud, rc):
            if rc != 0:
                self._status = DriverStatus.ERROR

        client.on_connect    = on_connect
        client.on_message    = on_message
        client.on_disconnect = on_disconnect
        client.connect_async(self._host, self._port, keepalive=opts.get("keepalive", 60))
        client.loop_start()
        self._client = client
        try:
            await asyncio.wait_for(connected_event.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            client.loop_stop()
            raise ConnectionError(f"MQTT timeout conectando em {self._host}:{self._port}")
        self._status = DriverStatus.CONNECTED
        self._last_error = None

    def _update(self, tag_id: str, value: float) -> None:
        self._latest[tag_id] = value
        self._ts_map[tag_id] = datetime.now(timezone.utc)

    async def disconnect(self) -> None:
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()
            self._client = None
        self._status = DriverStatus.DISCONNECTED

    async def read_all(self) -> list:
        readings = []
        for tag in self.cfg.tags:
            if tag.tag_id in self._latest:
                ts = self._ts_map.get(tag.tag_id, datetime.now(timezone.utc))
                readings.append(self._make_reading(tag, self._latest[tag.tag_id], ts=ts))
                self._read_count += 1
            else:
                readings.append(self._make_bad_reading(tag))
        return readings

    async def write_tag(self, tag_id: str, value: float) -> bool:
        tag = next((t for t in self.cfg.tags if t.tag_id == tag_id and t.writable), None)
        if not tag or not self._client:
            return False
        try:
            raw = value * tag.scale + tag.offset
            self._client.publish(tag.address, json.dumps({"value": raw}), qos=self.cfg.options.get("qos", 0))
            return True
        except Exception:
            return False
