"""Driver OPC UA — asyncua."""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from .base import DriverStatus, ProtocolDriver
from ..core.registry import DeviceConfig
from ..core.schemas import GatewayReading

log = logging.getLogger(__name__)


class OpcUaDriver(ProtocolDriver):
    PROTOCOL = "opcua"

    def __init__(self, cfg: DeviceConfig, gateway_id: str = "optiflow-gw-01"):
        super().__init__(cfg, gateway_id)
        self._client = None

    async def connect(self) -> None:
        from asyncua import Client
        self._status = DriverStatus.CONNECTING
        opts = self.cfg.options
        self._client = Client(url=self.cfg.endpoint, timeout=opts.get("session_timeout", 30_000) / 1000)
        await self._client.connect()
        self._status = DriverStatus.CONNECTED
        self._last_error = None

    async def disconnect(self) -> None:
        if self._client:
            try:
                await self._client.disconnect()
            except Exception:
                pass
            self._client = None
        self._status = DriverStatus.DISCONNECTED

    async def read_all(self) -> list[GatewayReading]:
        now = datetime.now(timezone.utc)
        readings = []
        failures = 0
        for tag in self.cfg.tags:
            try:
                node = self._client.get_node(tag.address)
                dv   = await node.read_data_value()
                raw  = float(dv.Value.Value)
                quality = "good" if dv.StatusCode.is_good() else ("uncertain" if dv.StatusCode.is_uncertain() else "bad")
                ts = dv.SourceTimestamp or now
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                readings.append(self._make_reading(tag, raw, quality=quality, ts=ts))
                self._read_count += 1
            except Exception as e:
                failures += 1
                self._error_count += 1
                self._last_error = str(e)
                self._last_failure_at = now
                readings.append(self._make_bad_reading(tag))
        if self.cfg.tags and failures == len(self.cfg.tags):
            self._status = DriverStatus.ERROR
            await self.disconnect()
            raise ConnectionError(
                f"opcua all tags failed for {self.cfg.device_id}; forcing reconnect"
            )
        if failures == 0:
            self._last_error = None
            self._last_success_at = now
        return readings

    async def write_tag(self, tag_id: str, value: float) -> bool:
        tag = next((t for t in self.cfg.tags if t.tag_id == tag_id and t.writable), None)
        if not tag or not self._client:
            return False
        try:
            from asyncua import ua
            node = self._client.get_node(tag.address)
            raw  = (value - tag.offset) / tag.scale
            await node.write_value(ua.DataValue(ua.Variant(float(raw), ua.VariantType.Double)))
            return True
        except Exception as e:
            log.error("opcua.write_error", extra={"device_id": self.cfg.device_id, "error": str(e)})
            return False
