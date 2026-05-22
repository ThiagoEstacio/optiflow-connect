"""EtherNet/IP Discovery Scanner — porta 44818 (CIP)."""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from .base import DiscoveryScanner
from .network import scan_subnet
from ..core.registry import DeviceConfig, TagConfig
log = logging.getLogger(__name__)
_ENIP_PORT = 44818
_NUMERIC_TYPES = {"REAL", "INT", "DINT", "SINT", "UINT", "UDINT", "USINT", "LREAL", "LINT", "ULINT"}

class EtherNetIpScanner(DiscoveryScanner):
    PROTOCOL = "enip"
    async def scan(self, target: str, slot: int = 0, timeout: float = 0.5, **kwargs) -> list[DeviceConfig]:
        devices = []
        async for ip in scan_subnet(target.split(":")[0], _ENIP_PORT, timeout=timeout):
            device = await _probe_enip(ip, slot)
            if device:
                devices.append(device)
        return devices

async def _probe_enip(ip: str, slot: int) -> DeviceConfig | None:
    try:
        import asyncio
        from functools import partial
        from pycomm3 import LogixDriver
        loop = asyncio.get_running_loop()
        plc  = LogixDriver(f"{ip}/{slot}", init_tags=False, init_program_tags=False)
        if not await loop.run_in_executor(None, plc.open): return None
        info         = plc.info or {}
        product_name = info.get("product_name", "")
        tags_result  = await loop.run_in_executor(None, plc.get_tag_list)
        await loop.run_in_executor(None, plc.close)
        tag_cfgs = [TagConfig(tag_id=getattr(t, "tag_name", ""), address=getattr(t, "tag_name", ""), options={"data_type": str(getattr(t, "data_type_name", ""))}) for t in (tags_result or []) if str(getattr(t, "data_type_name", "")).upper() in _NUMERIC_TYPES and getattr(t, "tag_name", "") and not getattr(t, "tag_name", "").startswith("__")]
        device_id = f"{product_name}-{ip.replace('.', '-')}".strip() if product_name else f"CLX-{ip.replace('.', '-')}"
        return DeviceConfig(device_id=device_id, device_type="PLC", protocol="enip", endpoint=ip, tags=tag_cfgs, options={"slot": slot, "product_name": product_name}, discovery_source=f"enip-scan:{ip}", created_at=datetime.now(timezone.utc))
    except Exception:
        return None
