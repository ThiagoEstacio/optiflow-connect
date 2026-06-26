"""ProtocolDriver — interface abstrata para todos os drivers de protocolo."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum

from ..core.registry import DeviceConfig, TagConfig
from ..core.schemas import GatewayReading

log = logging.getLogger(__name__)


class DriverStatus(str, Enum):
    DISCONNECTED = "disconnected"
    CONNECTING   = "connecting"
    CONNECTED    = "connected"
    ERROR        = "error"


class ProtocolDriver(ABC):
    PROTOCOL: str = ""

    def __init__(self, cfg: DeviceConfig, gateway_id: str = "optiflow-gw-01"):
        self.cfg         = cfg
        self.gateway_id  = gateway_id
        self._status     = DriverStatus.DISCONNECTED
        self._last_error: str | None = None
        self._read_count  = 0
        self._error_count = 0
        self._last_success_at: datetime | None = None
        self._last_failure_at: datetime | None = None

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def disconnect(self) -> None: ...

    @abstractmethod
    async def read_all(self) -> list[GatewayReading]: ...

    async def write_tag(self, tag_id: str, value: float) -> bool:
        return False

    @property
    def is_connected(self) -> bool:
        return self._status == DriverStatus.CONNECTED

    @property
    def status(self) -> DriverStatus:
        return self._status

    @property
    def last_error(self) -> str | None:
        return self._last_error

    def info(self) -> dict:
        return {
            "device_id":    self.cfg.device_id,
            "device_type":  self.cfg.device_type,
            "protocol":     self.cfg.protocol,
            "endpoint":     self.cfg.endpoint,
            "status":       self._status.value,
            "last_error":   self._last_error,
            "read_count":   self._read_count,
            "error_count":  self._error_count,
            "tag_count":    len(self.cfg.tags),
            "last_success_at": self._last_success_at.isoformat() if self._last_success_at else None,
            "last_failure_at": self._last_failure_at.isoformat() if self._last_failure_at else None,
        }

    def _make_reading(self, tag: TagConfig, value: float, quality: str = "good", ts: datetime | None = None) -> GatewayReading:
        return GatewayReading(
            ts=ts or datetime.now(timezone.utc),
            gateway_id=self.gateway_id,
            device_id=self.cfg.device_id,
            device_type=self.cfg.device_type,
            tag_id=tag.tag_id,
            value=round(value * tag.scale + tag.offset, 4),
            quality=quality,  # type: ignore[arg-type]
            unit=tag.unit,
            protocol=self.cfg.protocol,
        )

    def _make_bad_reading(self, tag: TagConfig) -> GatewayReading:
        return self._make_reading(tag, 0.0, quality="bad")
