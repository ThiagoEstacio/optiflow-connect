"""Scanner de auto-descoberta — interface base."""
from __future__ import annotations

from abc import ABC, abstractmethod
from ..core.registry import DeviceConfig


class DiscoveryScanner(ABC):
    PROTOCOL: str = ""

    @abstractmethod
    async def scan(self, target: str, **kwargs) -> list[DeviceConfig]: ...
