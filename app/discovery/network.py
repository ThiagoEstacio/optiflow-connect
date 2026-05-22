"""Utilitários de rede para os scanners de descoberta."""
from __future__ import annotations

import asyncio
import ipaddress
import logging
from typing import AsyncIterator

log = logging.getLogger(__name__)


async def port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True
    except (asyncio.TimeoutError, ConnectionRefusedError, OSError):
        return False


async def scan_subnet(cidr: str, port: int, timeout: float = 0.5, concurrency: int = 64) -> AsyncIterator[str]:
    try:
        network = ipaddress.ip_network(cidr, strict=False)
    except ValueError:
        network = ipaddress.ip_network(f"{cidr}/32", strict=False)
    sem   = asyncio.Semaphore(concurrency)
    hosts = list(network.hosts()) or [ipaddress.ip_address(cidr)]

    async def _check(ip):
        async with sem:
            if await port_open(str(ip), port, timeout=timeout):
                return str(ip)
            return None

    tasks = [asyncio.create_task(_check(ip)) for ip in hosts]
    for coro in asyncio.as_completed(tasks):
        result = await coro
        if result:
            yield result
