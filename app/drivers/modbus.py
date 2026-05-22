"""Driver Modbus TCP — pymodbus 3.x."""
from __future__ import annotations

import logging
import struct
from datetime import datetime, timezone

from .base import DriverStatus, ProtocolDriver
from ..core.registry import DeviceConfig, TagConfig

log = logging.getLogger(__name__)

_DEFAULT_PORT   = 502
_DEFAULT_SLAVE  = 1
_DEFAULT_TIMEOUT = 3


def _parse_address(address: str) -> tuple[str, int]:
    kind, _, raw = address.partition(":")
    return kind.upper(), int(raw) - 1


class ModbusDriver(ProtocolDriver):
    PROTOCOL = "modbus"

    def __init__(self, cfg: DeviceConfig, gateway_id: str = "optiflow-gw-01"):
        super().__init__(cfg, gateway_id)
        self._client = None
        host_port = cfg.endpoint.split(":")
        self._host    = host_port[0]
        self._port    = int(host_port[1]) if len(host_port) > 1 else _DEFAULT_PORT
        self._slave   = int(cfg.options.get("slave_id", _DEFAULT_SLAVE))
        self._timeout = int(cfg.options.get("timeout", _DEFAULT_TIMEOUT))

    async def connect(self) -> None:
        from pymodbus.client import AsyncModbusTcpClient
        self._status = DriverStatus.CONNECTING
        self._client = AsyncModbusTcpClient(host=self._host, port=self._port, timeout=self._timeout)
        await self._client.connect()
        if not self._client.connected:
            raise ConnectionError(f"Modbus TCP não conectou em {self._host}:{self._port}")
        self._status = DriverStatus.CONNECTED
        self._last_error = None

    async def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
        self._status = DriverStatus.DISCONNECTED

    async def read_all(self) -> list:
        now = datetime.now(timezone.utc)
        readings = []
        for tag in self.cfg.tags:
            try:
                raw = await self._read_tag(tag)
                readings.append(self._make_reading(tag, raw, ts=now))
                self._read_count += 1
            except Exception as e:
                self._error_count += 1
                self._last_error = str(e)
                readings.append(self._make_bad_reading(tag))
        return readings

    async def _read_tag(self, tag: TagConfig) -> float:
        kind, addr = _parse_address(tag.address)
        if kind == "HR":
            r = await self._client.read_holding_registers(addr, 1, slave=self._slave)
            return float(r.registers[0])
        if kind == "HRF":
            r = await self._client.read_holding_registers(addr, 2, slave=self._slave)
            raw = struct.pack(">HH", r.registers[0], r.registers[1])
            return struct.unpack(">f", raw)[0]
        if kind == "IR":
            r = await self._client.read_input_registers(addr, 1, slave=self._slave)
            return float(r.registers[0])
        if kind == "COIL":
            r = await self._client.read_coils(addr, 1, slave=self._slave)
            return 1.0 if r.bits[0] else 0.0
        if kind == "DI":
            r = await self._client.read_discrete_inputs(addr, 1, slave=self._slave)
            return 1.0 if r.bits[0] else 0.0
        raise ValueError(f"Tipo de endereço Modbus desconhecido: {kind}")

    async def write_tag(self, tag_id: str, value: float) -> bool:
        tag = next((t for t in self.cfg.tags if t.tag_id == tag_id and t.writable), None)
        if not tag or not self._client:
            return False
        try:
            kind, addr = _parse_address(tag.address)
            raw = (value - tag.offset) / tag.scale
            if kind == "HR":
                await self._client.write_register(addr, int(raw), slave=self._slave)
            elif kind == "HRF":
                packed = struct.pack(">f", raw)
                regs   = struct.unpack(">HH", packed)
                await self._client.write_registers(addr, list(regs), slave=self._slave)
            elif kind == "COIL":
                await self._client.write_coil(addr, bool(raw), slave=self._slave)
            else:
                return False
            return True
        except Exception as e:
            log.error("modbus.write_error", extra={"error": str(e)})
            return False
