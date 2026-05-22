"""MQTT Discovery Scanner — subscreve tópico curinga e mapeia dispositivos."""
from __future__ import annotations
import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from .base import DiscoveryScanner
from ..core.registry import DeviceConfig, TagConfig
log = logging.getLogger(__name__)

class MqttScanner(DiscoveryScanner):
    PROTOCOL = "mqtt"
    async def scan(self, target: str, listen_seconds: int = 15, device_level: int = 1, tag_level: int = 2, username: str | None = None, password: str | None = None, **kwargs) -> list[DeviceConfig]:
        import paho.mqtt.client as mqtt
        host_port = target.split(":")
        host = host_port[0]
        port = int(host_port[1]) if len(host_port) > 1 else 1883
        received: dict[str, str] = {}
        def on_connect(cl, _ud, _flags, rc):
            if rc == 0: cl.subscribe("#", qos=0)
        def on_message(_cl, _ud, msg):
            try: received[msg.topic] = msg.payload.decode(errors="replace")
            except Exception: pass
        client = mqtt.Client(client_id="optiflow-scanner")
        if username: client.username_pw_set(username, password or "")
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect_async(host, port, keepalive=60)
        client.loop_start()
        await asyncio.sleep(listen_seconds)
        client.loop_stop()
        client.disconnect()
        return _group_topics(received, device_level, tag_level, target)

def _group_topics(received, device_level, tag_level, source) -> list[DeviceConfig]:
    device_tags: dict[str, dict[str, str]] = defaultdict(dict)
    for topic in received:
        parts = topic.split("/")
        if len(parts) <= max(device_level, tag_level): continue
        device_tags[parts[device_level]][parts[tag_level]] = topic
    return [DeviceConfig(device_id=did, device_type="SENSOR", protocol="mqtt", endpoint=source.split(":")[0], tags=[TagConfig(tag_id=tid, address=t) for tid, t in tag_map.items()], discovery_source=f"mqtt-scan:{source}", created_at=datetime.now(timezone.utc)) for did, tag_map in device_tags.items()]
