"""Simulador de protocolo industrial — gera dados sintéticos para VRPs e reservatórios."""
from __future__ import annotations
import asyncio
import logging
import math
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import AsyncIterator
from ..core.schemas import GatewayReading
log = logging.getLogger(__name__)

@dataclass
class VRPConfig:
    device_id: str
    zone: str
    sp_nominal: float
    pm_nominal: float
    vz_nominal: float

@dataclass
class ReservoirConfig:
    device_id: str
    h_nominal: float
    h_min: float
    h_max: float
    q_in_nominal: float
    n_pumps: int

VRPS = [
    VRPConfig("VRP-SAO-CRM-001", "ZONA_BAIXA",  32.0, 55.0, 18.5),
    VRPConfig("VRP-SAO-CRM-002", "ZONA_BAIXA",  30.0, 52.0, 14.2),
    VRPConfig("VRP-SAO-CRM-003", "ZONA_MEDIA",  38.0, 62.0, 22.1),
    VRPConfig("VRP-SAO-CRM-004", "ZONA_MEDIA",  36.0, 58.0, 19.8),
    VRPConfig("VRP-SAO-CRM-005", "ZONA_MEDIA",  35.0, 57.0, 16.3),
    VRPConfig("VRP-SAO-CRM-006", "ZONA_ALTA",   42.0, 70.0, 12.4),
    VRPConfig("VRP-SAO-CRM-007", "ZONA_ALTA",   40.0, 68.0, 10.9),
    VRPConfig("VRP-SAO-CRM-008", "ZONA_ALTA",   41.0, 69.0, 11.6),
    VRPConfig("VRP-SAO-CRM-009", "ZONA_CENTRAL",35.0, 60.0, 25.0),
]
RESERVOIRS = [
    ReservoirConfig("CRAT-SAO-CARMO", 4.2, 1.0, 6.0, 180.0, 3),
    ReservoirConfig("RES-SAO-NORTE",  3.8, 0.5, 5.5, 95.0,  2),
]

@dataclass
class VRPState:
    cfg: VRPConfig
    pj: float = 0.0; pm: float = 0.0; vz: float = 0.0; pos: float = 50.0; sp: float = 0.0; online: bool = True
    _tick: int = field(default=0, repr=False)
    def __post_init__(self):
        self.pj = self.cfg.sp_nominal; self.pm = self.cfg.pm_nominal
        self.vz = self.cfg.vz_nominal; self.sp = self.cfg.sp_nominal
    def step(self, t: float) -> None:
        self._tick += 1
        hour = (t / 3600) % 24
        demand = 1.0 + 0.25 * math.sin((hour - 6) * math.pi / 9)
        self.pj += 0.1 * (self.sp * demand - self.pj) + random.gauss(0, 0.08)
        self.pj  = max(0.0, self.pj)
        self.pm  = self.pj + self.cfg.pm_nominal - self.cfg.sp_nominal + random.gauss(0, 0.15)
        self.pm  = max(self.pj, self.pm)
        self.vz  = self.cfg.vz_nominal * demand + random.gauss(0, 0.5)
        self.vz  = max(0.0, self.vz)
        if random.random() < 0.003:
            self.pj *= random.uniform(0.6, 0.85)

@dataclass
class ReservoirState:
    cfg: ReservoirConfig
    h: float = 0.0; q_in: float = 0.0; q_out: float = 0.0; pumps_on: int = 0
    _tick: int = field(default=0, repr=False)
    def __post_init__(self): self.h = self.cfg.h_nominal
    def step(self, t: float) -> None:
        self._tick += 1
        hour = (t / 3600) % 24
        demand = 1.0 + 0.2 * math.sin((hour - 8) * math.pi / 10)
        self.q_in  = self.cfg.q_in_nominal + random.gauss(0, 3.0)
        self.q_out = self.cfg.q_in_nominal * demand + random.gauss(0, 4.0)
        self.h = max(self.cfg.h_min, min(self.cfg.h_max, self.h + (self.q_in - self.q_out) * 0.001 + random.gauss(0, 0.01)))
        if self.h < self.cfg.h_min + 0.5: self.pumps_on = self.cfg.n_pumps
        elif self.h > self.cfg.h_max - 0.5: self.pumps_on = 0
        else: self.pumps_on = max(0, min(self.cfg.n_pumps, self.pumps_on + random.choice([-1, 0, 0, 0, 1])))

class IndustrialSimulator:
    def __init__(self, gateway_id: str = "optiflow-sim-01"):
        self.gateway_id = gateway_id
        self._vrps       = [VRPState(cfg=c) for c in VRPS]
        self._reservoirs = [ReservoirState(cfg=c) for c in RESERVOIRS]
        self._t = 0.0; self._running = False
    def apply_setpoint(self, device_id: str, value: float) -> bool:
        for vrp in self._vrps:
            if vrp.cfg.device_id == device_id:
                vrp.sp = max(0.0, min(100.0, value)); return True
        return False
    async def generate(self, interval_s: float = 2.0) -> AsyncIterator[list[GatewayReading]]:
        self._running = True
        while self._running:
            now = datetime.now(timezone.utc); self._t += interval_s; batch = []
            for vrp in self._vrps:
                vrp.step(self._t); quality = "good" if vrp.online else "bad"
                for tag_id, value, unit in [("pj", vrp.pj, "mca"), ("pm", vrp.pm, "mca"), ("vz", vrp.vz, "lps"), ("pos", vrp.pos, "%"), ("sp", vrp.sp, "mca")]:
                    batch.append(GatewayReading(ts=now, gateway_id=self.gateway_id, device_id=vrp.cfg.device_id, device_type="VRP", tag_id=tag_id, value=round(value, 3), quality=quality, unit=unit, protocol="sim"))
            for res in self._reservoirs:
                res.step(self._t)
                for tag_id, value, unit in [("h", res.h, "m"), ("q_in", res.q_in, "lps"), ("q_out", res.q_out, "lps"), ("bombas", float(res.pumps_on), "")]:
                    batch.append(GatewayReading(ts=now, gateway_id=self.gateway_id, device_id=res.cfg.device_id, device_type="RESERVOIR", tag_id=tag_id, value=round(value, 3), quality="good", unit=unit, protocol="sim"))
            yield batch
            await asyncio.sleep(interval_s)
    def stop(self) -> None: self._running = False
