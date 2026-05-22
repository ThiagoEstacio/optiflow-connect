"""Simulador MQTT — publica leituras no Mosquitto como dispositivos de campo reais."""
from __future__ import annotations
import asyncio
import json
import logging
import math
import os
import random
import signal
from contextlib import suppress
from datetime import datetime, timezone
from dataclasses import dataclass, field
import paho.mqtt.client as mqtt
log = logging.getLogger(__name__)

MQTT_HOST     = os.getenv("MQTT_HOST",     "localhost")
MQTT_PORT     = int(os.getenv("MQTT_PORT", "1883"))
PUBLISH_INTERVAL_S = float(os.getenv("PUBLISH_INTERVAL_S", "2.0"))
GATEWAY_API   = os.getenv("GATEWAY_API",   "http://gateway:8080")
TOPIC_PREFIX  = os.getenv("TOPIC_PREFIX",  "optiflow")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(name)s %(levelname)s %(message)s")

@dataclass
class VRPConfig:
    device_id: str; zone: str; sp_nominal: float; pm_nominal: float; vz_nominal: float

@dataclass
class ReservoirConfig:
    device_id: str; h_nominal: float; h_min: float; h_max: float; q_in_nominal: float; n_pumps: int

VRPS = [VRPConfig("VRP-SAO-CRM-001", "ZONA_BAIXA", 32.0, 55.0, 18.5), VRPConfig("VRP-SAO-CRM-002", "ZONA_BAIXA", 30.0, 52.0, 14.2), VRPConfig("VRP-SAO-CRM-003", "ZONA_MEDIA", 38.0, 62.0, 22.1), VRPConfig("VRP-SAO-CRM-004", "ZONA_MEDIA", 36.0, 58.0, 19.8), VRPConfig("VRP-SAO-CRM-005", "ZONA_MEDIA", 35.0, 57.0, 16.3), VRPConfig("VRP-SAO-CRM-006", "ZONA_ALTA", 42.0, 70.0, 12.4), VRPConfig("VRP-SAO-CRM-007", "ZONA_ALTA", 40.0, 68.0, 10.9), VRPConfig("VRP-SAO-CRM-008", "ZONA_ALTA", 41.0, 69.0, 11.6), VRPConfig("VRP-SAO-CRM-009", "ZONA_CENTRAL", 35.0, 60.0, 25.0)]
RESERVOIRS = [ReservoirConfig("CRAT-SAO-CARMO", 4.2, 1.0, 6.0, 180.0, 3), ReservoirConfig("RES-SAO-NORTE", 3.8, 0.5, 5.5, 95.0, 2)]

@dataclass
class VRPState:
    cfg: VRPConfig; pj: float = 0.0; pm: float = 0.0; vz: float = 0.0; pos: float = 50.0; sp: float = 0.0; _t: float = field(default=0.0, repr=False)
    def __post_init__(self): self.pj = self.cfg.sp_nominal; self.pm = self.cfg.pm_nominal; self.vz = self.cfg.vz_nominal; self.sp = self.cfg.sp_nominal
    def step(self, t: float) -> None:
        self._t = t; hour = (t / 3600) % 24; demand = 1.0 + 0.25 * math.sin((hour - 6) * math.pi / 9)
        self.pj += 0.1 * (self.sp * demand - self.pj) + random.gauss(0, 0.08); self.pj = max(0.0, self.pj)
        self.pm = self.pj + self.cfg.pm_nominal - self.cfg.sp_nominal + random.gauss(0, 0.15); self.pm = max(self.pj, self.pm)
        self.vz = self.cfg.vz_nominal * demand + random.gauss(0, 0.5); self.vz = max(0.0, self.vz)
        if random.random() < 0.003: self.pj *= random.uniform(0.6, 0.85)

@dataclass
class ReservoirState:
    cfg: ReservoirConfig; h: float = 0.0; q_in: float = 0.0; q_out: float = 0.0; pumps_on: int = 0
    def __post_init__(self): self.h = self.cfg.h_nominal
    def step(self, t: float) -> None:
        hour = (t / 3600) % 24; demand = 1.0 + 0.2 * math.sin((hour - 8) * math.pi / 10)
        self.q_in = self.cfg.q_in_nominal + random.gauss(0, 3.0)
        self.q_out = self.cfg.q_in_nominal * demand + random.gauss(0, 4.0)
        self.h = max(self.cfg.h_min, min(self.cfg.h_max, self.h + (self.q_in - self.q_out) * 0.001 + random.gauss(0, 0.01)))
        if self.h < self.cfg.h_min + 0.5: self.pumps_on = self.cfg.n_pumps
        elif self.h > self.cfg.h_max - 0.5: self.pumps_on = 0
        else: self.pumps_on = max(0, min(self.cfg.n_pumps, self.pumps_on + random.choice([-1, 0, 0, 0, 1])))

class SimMqttPublisher:
    def __init__(self):
        self._vrps = [VRPState(cfg=c) for c in VRPS]; self._reservoirs = [ReservoirState(cfg=c) for c in RESERVOIRS]
        self._t = 0.0; self._client: mqtt.Client | None = None; self._loop = None; self._connected = asyncio.Event()
    def _on_connect(self, client, _ud, _flags, rc):
        if rc == 0:
            for vrp in self._vrps: client.subscribe(f"{TOPIC_PREFIX}/{vrp.cfg.device_id}/cmd/setpoint", qos=1)
            self._loop.call_soon_threadsafe(self._connected.set)
    def _on_message(self, _client, _ud, msg):
        parts = msg.topic.split("/")
        if len(parts) < 4 or parts[-1] != "setpoint": return
        device_id = parts[1]
        try:
            payload = json.loads(msg.payload)
            value = float(payload.get("value", payload) if isinstance(payload, dict) else payload)
            for vrp in self._vrps:
                if vrp.cfg.device_id == device_id: vrp.sp = max(0.0, min(100.0, value)); return
        except Exception as e: log.error("sim_mqtt.cmd_error", extra={"error": str(e)})
    def _on_disconnect(self, _client, _ud, rc):
        if rc != 0: self._loop.call_soon_threadsafe(self._connected.clear)
    async def connect(self) -> None:
        self._loop = asyncio.get_running_loop()
        client = mqtt.Client(client_id="optiflow-sim-publisher")
        client.on_connect = self._on_connect; client.on_message = self._on_message; client.on_disconnect = self._on_disconnect
        client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60); client.loop_start(); self._client = client
        try: await asyncio.wait_for(self._connected.wait(), timeout=15.0)
        except asyncio.TimeoutError: raise ConnectionError(f"MQTT timeout: {MQTT_HOST}:{MQTT_PORT}")
    async def disconnect(self) -> None:
        if self._client: self._client.loop_stop(); self._client.disconnect(); self._client = None
    def _publish(self, device_id: str, tag_id: str, value: float) -> None:
        self._client.publish(f"{TOPIC_PREFIX}/{device_id}/{tag_id}", json.dumps({"value": round(value, 3), "ts": datetime.now(timezone.utc).isoformat(), "quality": "good"}), qos=0)
    async def run(self) -> None:
        while True:
            self._t += PUBLISH_INTERVAL_S
            for vrp in self._vrps:
                vrp.step(self._t)
                for tid, v in [("pj", vrp.pj), ("pm", vrp.pm), ("vz", vrp.vz), ("pos", vrp.pos), ("sp", vrp.sp)]: self._publish(vrp.cfg.device_id, tid, v)
            for res in self._reservoirs:
                res.step(self._t)
                for tid, v in [("h", res.h), ("q_in", res.q_in), ("q_out", res.q_out), ("bombas", float(res.pumps_on))]: self._publish(res.cfg.device_id, tid, v)
            await asyncio.sleep(PUBLISH_INTERVAL_S)

async def register_devices_with_gateway() -> None:
    import httpx
    vrp_tags = [{"tag_id": "pj", "address": "{p}/{id}/pj", "unit": "mca"}, {"tag_id": "pm", "address": "{p}/{id}/pm", "unit": "mca"}, {"tag_id": "vz", "address": "{p}/{id}/vz", "unit": "lps"}, {"tag_id": "pos", "address": "{p}/{id}/pos", "unit": "%"}, {"tag_id": "sp", "address": "{p}/{id}/sp", "unit": "mca", "writable": True}]
    res_tags  = [{"tag_id": "h", "address": "{p}/{id}/h", "unit": "m"}, {"tag_id": "q_in", "address": "{p}/{id}/q_in", "unit": "lps"}, {"tag_id": "q_out", "address": "{p}/{id}/q_out", "unit": "lps"}, {"tag_id": "bombas", "address": "{p}/{id}/bombas", "unit": ""}]
    endpoint = f"{GATEWAY_API}/api/devices"
    devices = [{"device_id": c.device_id, "device_type": "VRP", "protocol": "mqtt", "endpoint": f"{MQTT_HOST}:{MQTT_PORT}", "poll_interval_s": PUBLISH_INTERVAL_S, "tags": [{**t, "address": t["address"].format(p=TOPIC_PREFIX, id=c.device_id)} for t in vrp_tags]} for c in VRPS] + [{"device_id": c.device_id, "device_type": "RESERVOIR", "protocol": "mqtt", "endpoint": f"{MQTT_HOST}:{MQTT_PORT}", "poll_interval_s": PUBLISH_INTERVAL_S, "tags": [{**t, "address": t["address"].format(p=TOPIC_PREFIX, id=c.device_id)} for t in res_tags]} for c in RESERVOIRS]
    async with httpx.AsyncClient(timeout=5) as http:
        for attempt in range(20):
            try:
                for dev in devices:
                    r = await http.post(endpoint, json=dev)
                    if r.status_code in (200, 201, 409): log.info("sim_mqtt.registered", extra={"device_id": dev["device_id"]})
                return
            except Exception as e: log.warning("sim_mqtt.gateway_not_ready", extra={"attempt": attempt + 1, "error": str(e)}); await asyncio.sleep(3)

async def main() -> None:
    publisher = SimMqttPublisher(); stop_event = asyncio.Event(); loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM): loop.add_signal_handler(sig, lambda: stop_event.set())
    for attempt in range(10):
        try: await publisher.connect(); break
        except ConnectionError as e: log.warning("sim_mqtt.retry", extra={"attempt": attempt + 1, "error": str(e)}); await asyncio.sleep(5)
    else: return
    asyncio.create_task(register_devices_with_gateway())
    tasks = [asyncio.create_task(publisher.run(), name="sim-publisher")]
    stop_task = asyncio.create_task(stop_event.wait(), name="stop")
    await asyncio.wait([*tasks, stop_task], return_when=asyncio.FIRST_COMPLETED)
    stop_task.cancel()
    for t in tasks: t.cancel(); 
    with suppress(asyncio.CancelledError): await asyncio.gather(*tasks, return_exceptions=True)
    await publisher.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
