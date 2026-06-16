"""OptiFlow Connect — entry point.

Serviços concorrentes:
  1. DriverManager   — polling de todos os devices, publica gateway:readings
  2. CommandConsumer — consome opera:commands, roteia via DriverManager
  3. Management API  — FastAPI REST na porta 8080 (config em runtime)
  4. Health heartbeat — /app/data/gateway.health a cada 10 s

Responsabilidade de persistência:
  O gateway publica em gateway:readings (Redis Streams) e encerra.
  Quem persiste é o consumidor: OptiFlow Historian.
  O gateway NÃO escreve em nenhum banco de dados diretamente.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import threading
import time
from contextlib import suppress
from pathlib import Path

import uvicorn

from .core.bus import GatewayBus
from .core.registry import DeviceRegistry
from .services.driver_manager import DriverManager
from .services.command_consumer import CommandConsumer
from .api.app import management_app, init_api

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

REDIS_URL   = os.getenv("REDIS_URL",    "redis://localhost:6379/1")
GATEWAY_ID  = os.getenv("GATEWAY_ID",  "optiflow-gateway-01")
DB_PATH     = os.getenv("DEVICES_DB",  "/app/data/devices.db")
MGMT_PORT   = int(os.getenv("MGMT_PORT", "8080"))
HEALTH_FILE = Path(os.getenv("HEALTH_FILE", "/app/data/gateway.health"))

# Watchdog-timer de software: limiar e cadência (configuráveis).
WATCHDOG_STUCK_S = float(os.getenv("WATCHDOG_STUCK_S", "30"))
WATCHDOG_CHECK_S = float(os.getenv("WATCHDOG_CHECK_S", "5"))

# Batimento do event loop. Atualizado por _liveness_beat (no loop) e lido pela
# thread de SO _loop_watchdog (fora do loop).
_LIVENESS = {"ts": time.monotonic()}


async def _liveness_beat(interval_s: float = 2.0) -> None:
    """Marca que o event loop está vivo e progredindo."""
    while True:
        _LIVENESS["ts"] = time.monotonic()
        await asyncio.sleep(interval_s)


def _start_loop_watchdog() -> None:
    """Watchdog-timer em software (padrão de watchdog de hardware).

    Uma thread de SO — que continua rodando mesmo se o event loop asyncio
    travar — vigia o batimento do loop. Se o loop não avança por
    WATCHDOG_STUCK_S, o processo é MORTO (os._exit) e o Docker o reinicia via
    `restart: unless-stopped`. Converte um hang silencioso (que mantinha o
    container "vivo" mas congelado) em recuperação determinística e rápida,
    sem depender de watchdog externo nem do socket do Docker.
    """
    def _run() -> None:
        while True:
            time.sleep(WATCHDOG_CHECK_S)
            lag = time.monotonic() - _LIVENESS["ts"]
            if lag > WATCHDOG_STUCK_S:
                log.critical(
                    "gateway.loop_stuck lag=%.1fs > %.0fs — os._exit(1) p/ Docker reiniciar",
                    lag, WATCHDOG_STUCK_S,
                )
                os._exit(1)

    threading.Thread(target=_run, name="loop-watchdog", daemon=True).start()
    log.info("gateway.loop_watchdog_armado stuck_after=%.0fs check=%.0fs", WATCHDOG_STUCK_S, WATCHDOG_CHECK_S)


async def _health_loop(
    driver_manager: DriverManager,
    interval_s: float = 10.0,
    stale_after_s: float = 90.0,
) -> None:
    """Liveness HONESTA: o arquivo de saúde só é mantido fresco enquanto o
    gateway está de fato PUBLICANDO. Se a ingestão estola (loop preso, drivers
    mortos), published_total para de avançar -> paramos de tocar o arquivo ->
    ele envelhece -> o healthcheck (que checa mtime) marca unhealthy.
    Acaba com o 'verde mentiroso' (antes tocava incondicionalmente)."""
    HEALTH_FILE.parent.mkdir(parents=True, exist_ok=True)
    HEALTH_FILE.touch()  # satisfaz o start_period
    last_total = -1
    last_progress = time.monotonic()
    while True:
        await asyncio.sleep(interval_s)
        total = driver_manager.status().get("published_total", 0)
        now = time.monotonic()
        if total != last_total:
            last_total = total
            last_progress = now
        if now - last_progress < stale_after_s:
            HEALTH_FILE.touch()
        else:
            log.warning("gateway.health_stale published_total parado há %.0fs", now - last_progress)


async def _run_management_api() -> None:
    config = uvicorn.Config(
        management_app, host="0.0.0.0", port=MGMT_PORT,
        log_level="warning", access_log=False,
    )
    server = uvicorn.Server(config)
    await server.serve()


async def main() -> None:
    bus = GatewayBus(redis_url=REDIS_URL, gateway_id=GATEWAY_ID)
    await bus.connect()

    registry = DeviceRegistry(db_path=DB_PATH)
    await registry.load()

    driver_manager = DriverManager(bus, registry, gateway_id=GATEWAY_ID)
    await driver_manager.start()

    cmd_consumer = CommandConsumer(bus, driver_manager)
    init_api(registry, driver_manager)

    # Watchdog-timer interno: mata o processo se o event loop travar -> Docker
    # reinicia. Defesa primária contra o hang intermitente do gateway.
    _start_loop_watchdog()

    loop       = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: stop_event.set())

    tasks = [
        asyncio.create_task(_liveness_beat(),      name="liveness"),
        asyncio.create_task(cmd_consumer.run(),    name="cmd-consumer"),
        asyncio.create_task(_run_management_api(), name="mgmt-api"),
        asyncio.create_task(_health_loop(driver_manager), name="health"),
    ]

    stop_task = asyncio.create_task(stop_event.wait(), name="stop")
    done, _   = await asyncio.wait([*tasks, stop_task], return_when=asyncio.FIRST_COMPLETED)

    for t in done:
        if t.get_name() != "stop":
            exc = t.exception() if not t.cancelled() else None
            if exc:
                log.error("gateway.task_crashed", extra={"task": t.get_name(), "error": str(exc)})

    await driver_manager.stop()
    stop_task.cancel()
    for t in tasks:
        t.cancel()
        with suppress(asyncio.CancelledError):
            await t
    await bus.close()


if __name__ == "__main__":
    asyncio.run(main())
