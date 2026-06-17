"""DriverManager — orquestra todos os drivers de protocolo."""
from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress
from typing import Type

from ..core.bus import GatewayBus
from ..core.registry import DeviceConfig, DeviceRegistry
from ..core.schemas import GatewayReading
from ..drivers.base import DriverStatus, ProtocolDriver

log = logging.getLogger(__name__)

_BACKOFF_INITIAL = 5.0
_BACKOFF_MAX     = 60.0


def _build_driver_registry() -> dict[str, Type[ProtocolDriver]]:
    registry: dict[str, Type[ProtocolDriver]] = {}
    try:
        from ..drivers.opcua import OpcUaDriver
        registry["opcua"] = OpcUaDriver
    except ImportError:
        pass
    try:
        from ..drivers.modbus import ModbusDriver
        registry["modbus"] = ModbusDriver
    except ImportError:
        pass
    try:
        from ..drivers.mqtt_drv import MqttDriver
        registry["mqtt"] = MqttDriver
    except ImportError:
        pass
    try:
        from ..drivers.s7 import S7Driver
        registry["s7"] = S7Driver
    except ImportError:
        pass
    try:
        from ..drivers.enip import EtherNetIpDriver
        registry["enip"] = EtherNetIpDriver
    except ImportError:
        pass
    from ..drivers.simulator import SimulatorDriver
    registry["sim"] = SimulatorDriver
    return registry


_DRIVER_REGISTRY = _build_driver_registry()


class DriverManager:
    def __init__(self, bus: GatewayBus, registry: DeviceRegistry, gateway_id: str = "optiflow-gw-01", max_batch_size: int = 50):
        self._bus          = bus
        self._registry     = registry
        self._gateway_id   = gateway_id
        self._max_batch    = max_batch_size
        self._drivers:  dict[str, ProtocolDriver]  = {}
        self._tasks:    dict[str, asyncio.Task]    = {}
        self._published_total = 0
        self._errors_total    = 0

    async def start(self, exclude_protocols: set[str] | None = None) -> None:
        exclude = exclude_protocols or set()
        devices = [d for d in self._registry.list_devices() if d.protocol not in exclude]
        if not devices:
            if not exclude:
                from ..drivers.simulator import DEFAULT_SIM_CONFIG
                await self.add_device(DEFAULT_SIM_CONFIG)
        else:
            for cfg in devices:
                if cfg.enabled:
                    await self.add_device(cfg)

    async def stop(self) -> None:
        for task in list(self._tasks.values()):
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        for driver in list(self._drivers.values()):
            with suppress(Exception):
                await driver.disconnect()
        self._drivers.clear()
        self._tasks.clear()

    async def add_device(self, cfg: DeviceConfig) -> bool:
        driver_cls = _DRIVER_REGISTRY.get(cfg.protocol)
        if not driver_cls:
            return False
        if cfg.device_id in self._drivers:
            await self.remove_device(cfg.device_id)
        driver = driver_cls(cfg, gateway_id=self._gateway_id)
        self._drivers[cfg.device_id] = driver
        task = asyncio.create_task(self._poll_loop(driver), name=f"drv:{cfg.device_id}")
        self._tasks[cfg.device_id] = task
        return True

    async def remove_device(self, device_id: str) -> bool:
        task = self._tasks.pop(device_id, None)
        if task:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        driver = self._drivers.pop(device_id, None)
        if driver:
            with suppress(Exception):
                await driver.disconnect()
            return True
        return False

    async def reload_device(self, cfg: DeviceConfig) -> bool:
        await self.remove_device(cfg.device_id)
        return await self.add_device(cfg)

    async def write_tag(self, device_id: str, tag_id: str, value: float) -> bool:
        driver = self._drivers.get(device_id)
        if driver and driver.is_connected:
            return await driver.write_tag(tag_id, value)
        for d in self._drivers.values():
            if d.cfg.protocol == "sim":
                from ..drivers.simulator import SimulatorDriver
                if isinstance(d, SimulatorDriver):
                    return d.apply_setpoint(device_id, value)
        return False

    def status(self) -> dict:
        return {
            "drivers":         {did: d.info() for did, d in self._drivers.items()},
            "published_total": self._published_total,
            "errors_total":    self._errors_total,
            "protocols":       list(_DRIVER_REGISTRY.keys()),
        }

    def get_driver_status(self, device_id: str) -> dict | None:
        d = self._drivers.get(device_id)
        return d.info() if d else None

    async def _poll_loop(self, driver: ProtocolDriver) -> None:
        backoff = _BACKOFF_INITIAL
        cfg     = driver.cfg
        while True:
            if not driver.is_connected:
                driver._status = DriverStatus.CONNECTING
                try:
                    await driver.connect()
                    backoff = _BACKOFF_INITIAL
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    driver._status = DriverStatus.ERROR
                    driver._last_error = str(e)
                    driver._error_count += 1
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, _BACKOFF_MAX)
                    continue
            t0 = time.monotonic()
            try:
                readings = await driver.read_all()
                if readings:
                    await self._publish(readings)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                driver._status = DriverStatus.ERROR
                driver._last_error = str(e)
                driver._error_count += 1
                self._errors_total += 1
                with suppress(Exception):
                    await driver.disconnect()
                continue
            elapsed = time.monotonic() - t0
            await asyncio.sleep(max(0.0, cfg.poll_interval_s - elapsed))

    async def _publish(self, readings: list[GatewayReading]) -> None:
        for i in range(0, len(readings), self._max_batch):
            batch = readings[i : i + self._max_batch]
            try:
                await self._bus.publish_readings_batch(batch)
                self._published_total += len(batch)
            except Exception as e:
                self._errors_total += 1
                await asyncio.sleep(2)
