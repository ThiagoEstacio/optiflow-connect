"""Driver EtherNet/IP — Allen-Bradley / Rockwell via pycomm3."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from functools import partial

from .base import DriverStatus, ProtocolDriver
from ..core.registry import DeviceConfig

log = logging.getLogger(__name__)


class EtherNetIpDriver(ProtocolDriver):
    PROTOCOL = "enip"

    def __init__(self, cfg: DeviceConfig, gateway_id: str = "optiflow-gw-01"):
        super().__init__(cfg, gateway_id)
        self._plc  = None
        self._loop: asyncio.AbstractEventLoop | None = None

    async def connect(self) -> None:
        from pycomm3 import LogixDriver
        self._status = DriverStatus.CONNECTING
        self._loop   = asyncio.get_running_loop()
        slot = self.cfg.options.get("slot", 0)
        path = f"{self.cfg.endpoint}/{slot}"
        plc  = LogixDriver(path, init_tags=False, init_program_tags=False)
        if not await self._loop.run_in_executor(None, plc.open):
            raise ConnectionError(f"EtherNet/IP: não conectou em {path}")
        self._plc    = plc
        self._status = DriverStatus.CONNECTED
        self._last_error = None

    async def disconnect(self) -> None:
        if self._plc:
            try:
                await asyncio.get_running_loop().run_in_executor(None, self._plc.close)
            except Exception:
                pass
            self._plc = None
        self._status = DriverStatus.DISCONNECTED

    async def read_all(self) -> list:
        now      = datetime.now(timezone.utc)
        readings = []
        tags_to_read = [tag.address for tag in self.cfg.tags]
        if not tags_to_read:
            return readings
        try:
            results = await asyncio.get_running_loop().run_in_executor(None, partial(self._plc.read, *tags_to_read))
            if not isinstance(results, (list, tuple)):
                results = [results]
            for tag_cfg, result in zip(self.cfg.tags, results):
                if result.error:
                    readings.append(self._make_bad_reading(tag_cfg))
                else:
                    try:
                        readings.append(self._make_reading(tag_cfg, float(result.value), ts=now))
                        self._read_count += 1
                    except (TypeError, ValueError):
                        readings.append(self._make_bad_reading(tag_cfg))
        except Exception as e:
            self._error_count += 1
            self._last_error = str(e)
            for tag_cfg in self.cfg.tags:
                readings.append(self._make_bad_reading(tag_cfg))
        return readings

    async def write_tag(self, tag_id: str, value: float) -> bool:
        tag = next((t for t in self.cfg.tags if t.tag_id == tag_id and t.writable), None)
        if not tag or not self._plc:
            return False
        try:
            raw    = (value - tag.offset) / tag.scale
            result = await asyncio.get_running_loop().run_in_executor(None, partial(self._plc.write, (tag.address, raw)))
            return not result.error if result else False
        except Exception:
            return False
