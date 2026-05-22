"""SimulatorDriver — driver que usa o IndustrialSimulator como fonte de dados."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from .base import DriverStatus, ProtocolDriver
from ..core.registry import DeviceConfig
from ..core.schemas import GatewayReading
from ..protocols.simulator import IndustrialSimulator

log = logging.getLogger(__name__)

DEFAULT_SIM_CONFIG = DeviceConfig(
    device_id="optiflow-sim-01",
    device_type="SIMULATOR",
    protocol="sim",
    endpoint="internal",
    enabled=True,
    poll_interval_s=2.0,
    options={"gateway_id": "optiflow-sim-01"},
)


class SimulatorDriver(ProtocolDriver):
    PROTOCOL = "sim"

    def __init__(self, cfg: DeviceConfig, gateway_id: str = "optiflow-gw-01"):
        super().__init__(cfg, gateway_id)
        sim_id = cfg.options.get("gateway_id", gateway_id)
        self._sim = IndustrialSimulator(gateway_id=sim_id)

    async def connect(self) -> None:
        self._status = DriverStatus.CONNECTED
        self._last_error = None

    async def disconnect(self) -> None:
        self._sim.stop()
        self._status = DriverStatus.DISCONNECTED

    async def read_all(self) -> list[GatewayReading]:
        self._sim._t += self.cfg.poll_interval_s
        now = datetime.now(timezone.utc)
        batch: list[GatewayReading] = []
        for vrp in self._sim._vrps:
            vrp.step(self._sim._t)
            quality = "good" if vrp.online else "bad"
            for tag_id, value, unit in [("pj", vrp.pj, "mca"), ("pm", vrp.pm, "mca"), ("vz", vrp.vz, "lps"), ("pos", vrp.pos, "%"), ("sp", vrp.sp, "mca")]:
                batch.append(GatewayReading(ts=now, gateway_id=self._sim.gateway_id, device_id=vrp.cfg.device_id, device_type="VRP", tag_id=tag_id, value=round(value, 3), quality=quality, unit=unit, protocol="sim"))
        for res in self._sim._reservoirs:
            res.step(self._sim._t)
            for tag_id, value, unit in [("h", res.h, "m"), ("q_in", res.q_in, "lps"), ("q_out", res.q_out, "lps"), ("bombas", float(res.pumps_on), "")]:
                batch.append(GatewayReading(ts=now, gateway_id=self._sim.gateway_id, device_id=res.cfg.device_id, device_type="RESERVOIR", tag_id=tag_id, value=round(value, 3), quality="good", unit=unit, protocol="sim"))
        self._read_count += len(batch)
        return batch

    async def write_tag(self, tag_id: str, value: float) -> bool:
        if tag_id != "sp":
            return False
        return self._sim.apply_setpoint(self.cfg.device_id, value)

    def apply_setpoint(self, device_id: str, value: float) -> bool:
        return self._sim.apply_setpoint(device_id, value)
