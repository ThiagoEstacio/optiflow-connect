"""Modbus TCP Discovery Scanner."""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from .base import DiscoveryScanner
from .network import scan_subnet
from ..core.registry import DeviceConfig
log = logging.getLogger(__name__)

class ModbusScanner(DiscoveryScanner):
    PROTOCOL = "modbus"
    async def scan(self, target: str, timeout: float = 0.5, slave_id: int = 1, **kwargs) -> list[DeviceConfig]:
        host = target.split(":")[0]
        port = int(target.split(":")[1]) if ":" in target else 502
        devices = []
        async for ip in scan_subnet(host, port, timeout=timeout):
            device = await _probe_modbus(ip, port, slave_id)
            if device:
                devices.append(device)
        return devices

async def _probe_modbus(ip: str, port: int, slave_id: int) -> DeviceConfig | None:
    try:
        from pymodbus.client import AsyncModbusTcpClient
        client = AsyncModbusTcpClient(host=ip, port=port, timeout=3)
        await client.connect()
        if not client.connected: return None
        vendor = model = ""
        try:
            req  = await client.read_device_information(slave=slave_id)
            info = req.information if hasattr(req, "information") else {}
            vendor = info.get(0, b"").decode(errors="replace")
            model  = info.get(1, b"").decode(errors="replace")
        except Exception:
            pass
        r = await client.read_holding_registers(0, 10, slave=slave_id)
        client.close()
        if r.isError(): return None
        device_id = f"{model.strip()}-{ip.replace('.', '-')}" if model else f"MODBUS-{ip.replace('.', '-')}"
        return DeviceConfig(device_id=device_id, device_type="SENSOR", protocol="modbus", endpoint=f"{ip}:{port}", tags=[], options={"slave_id": slave_id, "vendor": vendor, "model": model}, discovery_source=f"modbus-scan:{ip}", created_at=datetime.now(timezone.utc))
    except Exception:
        return None
