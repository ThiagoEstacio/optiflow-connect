"""Driver Siemens S7 — python-snap7."""
from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from functools import partial

from .base import DriverStatus, ProtocolDriver
from ..core.registry import DeviceConfig, TagConfig

log = logging.getLogger(__name__)

_DB_DBD  = re.compile(r"DB(\d+)\.DBD(\d+)", re.I)
_DB_DBW  = re.compile(r"DB(\d+)\.DBW(\d+)", re.I)
_DB_DBB  = re.compile(r"DB(\d+)\.DBB(\d+)", re.I)
_DB_DBX  = re.compile(r"DB(\d+)\.DBX(\d+)\.(\d+)", re.I)
_MERKER  = re.compile(r"M(\d+)\.(\d+)", re.I)


def _read_s7_value(client, address: str) -> float:
    import snap7
    from snap7 import util
    m = _DB_DBD.fullmatch(address)
    if m:
        db, byte = int(m.group(1)), int(m.group(2))
        return util.get_real(client.db_read(db, byte, 4), 0)
    m = _DB_DBW.fullmatch(address)
    if m:
        db, byte = int(m.group(1)), int(m.group(2))
        return float(util.get_int(client.db_read(db, byte, 2), 0))
    m = _DB_DBB.fullmatch(address)
    if m:
        db, byte = int(m.group(1)), int(m.group(2))
        return float(client.db_read(db, byte, 1)[0])
    m = _DB_DBX.fullmatch(address)
    if m:
        db, byte, bit = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return 1.0 if util.get_bool(client.db_read(db, byte, 1), 0, bit) else 0.0
    m = _MERKER.fullmatch(address)
    if m:
        byte, bit = int(m.group(1)), int(m.group(2))
        return 1.0 if util.get_bool(client.mb_read(byte, 1), 0, bit) else 0.0
    raise ValueError(f"Endereço S7 inválido: {address}")


class S7Driver(ProtocolDriver):
    PROTOCOL = "s7"

    def __init__(self, cfg: DeviceConfig, gateway_id: str = "optiflow-gw-01"):
        super().__init__(cfg, gateway_id)
        self._client = None
        self._loop: asyncio.AbstractEventLoop | None = None

    async def connect(self) -> None:
        import snap7
        self._status = DriverStatus.CONNECTING
        self._loop   = asyncio.get_running_loop()
        opts = self.cfg.options
        client = snap7.client.Client()
        await self._loop.run_in_executor(None, partial(client.connect, self.cfg.endpoint, opts.get("rack", 0), opts.get("slot", 1), opts.get("port", 102)))
        if not client.get_connected():
            raise ConnectionError(f"S7 não conectou em {self.cfg.endpoint}")
        self._client = client
        self._status = DriverStatus.CONNECTED
        self._last_error = None

    async def disconnect(self) -> None:
        if self._client:
            try:
                await asyncio.get_running_loop().run_in_executor(None, self._client.disconnect)
            except Exception:
                pass
            self._client = None
        self._status = DriverStatus.DISCONNECTED

    async def read_all(self) -> list:
        now = datetime.now(timezone.utc)
        readings = []
        for tag in self.cfg.tags:
            try:
                raw = await asyncio.get_running_loop().run_in_executor(None, partial(_read_s7_value, self._client, tag.address))
                readings.append(self._make_reading(tag, raw, ts=now))
                self._read_count += 1
            except Exception as e:
                self._error_count += 1
                self._last_error = str(e)
                readings.append(self._make_bad_reading(tag))
        return readings

    async def write_tag(self, tag_id: str, value: float) -> bool:
        tag = next((t for t in self.cfg.tags if t.tag_id == tag_id and t.writable), None)
        if not tag or not self._client:
            return False
        try:
            raw = (value - tag.offset) / tag.scale
            from snap7 import util
            m = _DB_DBD.fullmatch(tag.address)
            if m:
                db, byte = int(m.group(1)), int(m.group(2))
                data = bytearray(4)
                util.set_real(data, 0, raw)
                await asyncio.get_running_loop().run_in_executor(None, partial(self._client.db_write, db, byte, data))
                return True
            return False
        except Exception:
            return False
