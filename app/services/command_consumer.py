"""Command Consumer do Gateway: consome opera:commands e aplica no campo."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from ..core.bus import GatewayBus
from ..core.schemas import OperaCommand

log = logging.getLogger(__name__)


class CommandConsumer:
    def __init__(self, bus: GatewayBus, driver_manager, consumer_name: str = "gw-cmd-consumer-01"):
        self._bus           = bus
        self._dm            = driver_manager
        self._consumer_name = consumer_name
        self._executed      = 0
        self._failed        = 0

    async def run(self) -> None:
        async for cmd, ack_fn in self._bus.consume_commands(
            consumer_name=self._consumer_name, batch_size=5, block_ms=3_000,
        ):
            await self._handle(cmd, ack_fn)

    async def _handle(self, cmd: OperaCommand, ack_fn) -> None:
        age_s = (datetime.now(timezone.utc) - cmd.ts).total_seconds()
        if age_s > cmd.ttl_s:
            await ack_fn(success=False, error=f"TTL expirado: {age_s:.1f}s > {cmd.ttl_s}s")
            return
        try:
            applied = await self._execute(cmd)
            self._executed += 1
            await ack_fn(success=True, applied_value=applied)
        except Exception as e:
            self._failed += 1
            await ack_fn(success=False, error=str(e))

    async def _execute(self, cmd: OperaCommand) -> float | None:
        action = cmd.action
        params = cmd.params
        if action == "set_setpoint":
            value = float(params.get("value", 0))
            ok    = await self._dm.write_tag(cmd.device_id, "sp", value)
            if not ok:
                raise ValueError(f"Dispositivo '{cmd.device_id}' não encontrado ou não suporta escrita")
            await asyncio.sleep(0.05)
            return value
        if action in ("pump_on", "pump_off"):
            state = action == "pump_on"
            await self._dm.write_tag(cmd.device_id, "pump_state", float(state))
            return 1.0 if state else 0.0
        if action == "set_mode":
            mode = params.get("mode", "auto")
            await self._dm.write_tag(cmd.device_id, "mode", 1.0 if mode == "auto" else 0.0)
            return None
        if action == "set_param":
            name  = params.get("name", "")
            value = params.get("value")
            if value is not None:
                await self._dm.write_tag(cmd.device_id, name, float(value))
            return float(value) if value is not None else None
        raise ValueError(f"Ação desconhecida: {action}")

    @property
    def stats(self) -> dict:
        return {"executed": self._executed, "failed": self._failed}
