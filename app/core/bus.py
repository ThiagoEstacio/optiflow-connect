"""GatewayBus — barramento Redis Streams entre Connect e OPERA.

Publica leituras em gateway:readings (XADD).
Consome comandos de opera:commands (XREADGROUP) e ack em gateway:acks.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Callable, Awaitable

import redis.asyncio as aioredis

from .schemas import (
    GatewayReading, OperaCommand, GatewayAck,
    STREAM_READINGS, STREAM_COMMANDS, STREAM_ACKS,
    GROUP_GATEWAY,
)

log = logging.getLogger(__name__)

_MAXLEN = 50_000


class GatewayBus:
    def __init__(self, redis_url: str, gateway_id: str):
        self._url        = redis_url
        self._gateway_id = gateway_id
        self._redis: aioredis.Redis | None = None

    async def connect(self) -> None:
        # health_check_interval + retry: o cliente revalida a conexão
        # periodicamente e reconecta sozinho após blip/restart do redis,
        # em vez de falhar em silêncio.
        self._redis = aioredis.from_url(
            self._url,
            decode_responses=True,
            health_check_interval=15,
            socket_keepalive=True,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )
        await self._redis.ping()
        await self._ensure_groups()
        log.info("gateway_bus.connected", extra={"redis": self._url})

    async def close(self) -> None:
        if self._redis:
            await self._redis.aclose()
            self._redis = None

    # ── publish ───────────────────────────────────────────────────────────────

    async def publish_readings_batch(self, readings: list[GatewayReading]) -> int:
        if not self._redis or not readings:
            return 0
        pipe = self._redis.pipeline(transaction=False)
        for r in readings:
            pipe.xadd(STREAM_READINGS, r.to_stream_entry(), maxlen=_MAXLEN, approximate=True)
        await pipe.execute()
        return len(readings)

    # ── consume commands ──────────────────────────────────────────────────────

    async def consume_commands(
        self,
        consumer_name: str,
        batch_size: int = 5,
        block_ms: int = 3_000,
    ) -> AsyncIterator[tuple[OperaCommand, Callable[..., Awaitable[None]]]]:
        """Async generator que produz (OperaCommand, ack_fn) para cada mensagem."""
        if not self._redis:
            return

        while True:
            try:
                results = await self._redis.xreadgroup(
                    GROUP_GATEWAY,
                    consumer_name,
                    {STREAM_COMMANDS: ">"},
                    count=batch_size,
                    block=block_ms,
                )
            except aioredis.ConnectionError as exc:
                log.error("gateway_bus.redis_error", extra={"error": str(exc)})
                await asyncio.sleep(5)
                continue

            if not results:
                continue

            for _stream, messages in results:
                for redis_id, fields in messages:
                    t0 = time.monotonic()
                    try:
                        cmd = OperaCommand.from_stream_entry(fields)
                    except Exception as exc:
                        log.warning(
                            "gateway_bus.bad_command",
                            extra={"redis_id": redis_id, "error": str(exc)},
                        )
                        await self._redis.xack(STREAM_COMMANDS, GROUP_GATEWAY, redis_id)
                        continue

                    async def _ack(
                        *,
                        success: bool,
                        error: str | None = None,
                        applied_value: float | None = None,
                        _rid: str = redis_id,
                        _cmd: OperaCommand = cmd,
                        _t0: float = t0,
                    ) -> None:
                        latency_ms = (time.monotonic() - _t0) * 1000
                        ack = GatewayAck(
                            command_msg_id=_cmd.msg_id,
                            proposal_id=_cmd.proposal_id,
                            device_id=_cmd.device_id,
                            action=_cmd.action,
                            success=success,
                            error=error,
                            applied_value=applied_value,
                            latency_ms=latency_ms,
                        )
                        if self._redis:
                            await self._redis.xack(STREAM_COMMANDS, GROUP_GATEWAY, _rid)
                            await self._redis.xadd(
                                STREAM_ACKS,
                                ack.to_stream_entry(),
                                maxlen=10_000,
                                approximate=True,
                            )

                    yield cmd, _ack

    # ── helpers ───────────────────────────────────────────────────────────────

    async def _ensure_groups(self) -> None:
        for stream, group in [
            (STREAM_COMMANDS, GROUP_GATEWAY),
            (STREAM_READINGS, "historian-writer"),
        ]:
            try:
                await self._redis.xgroup_create(stream, group, id="$", mkstream=True)
            except aioredis.ResponseError as exc:
                if "BUSYGROUP" not in str(exc):
                    log.warning(
                        "gateway_bus.group_error",
                        extra={"stream": stream, "group": group, "error": str(exc)},
                    )
