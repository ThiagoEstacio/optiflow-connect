"""Gateway Management API — FastAPI REST para configuração em runtime."""
from __future__ import annotations
import logging
from typing import Any
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from ..core.registry import DeviceConfig, DeviceRegistry, TagConfig
log = logging.getLogger(__name__)

management_app = FastAPI(title="OptiFlow Connect", version="2.0.0")
management_app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
_registry: DeviceRegistry | None = None
_driver_manager = None

def init_api(registry: DeviceRegistry, driver_manager) -> None:
    global _registry, _driver_manager
    _registry = registry; _driver_manager = driver_manager

class DeviceAddRequest(BaseModel):
    device_id: str; device_type: str; protocol: str; endpoint: str
    enabled: bool = True; poll_interval_s: float = 2.0
    tags: list[TagConfig] = []; options: dict[str, Any] = {}

class EnableRequest(BaseModel):
    enabled: bool

class ScanRequest(BaseModel):
    protocol: str; target: str
    listen_seconds: int = 15; max_depth: int = 2; slave_id: int = 1; timeout: float = 0.5

@management_app.get("/api/devices")
async def list_devices():
    if not _registry: raise HTTPException(503, "Not initialized")
    return [{**cfg.model_dump(), "runtime": _driver_manager.get_driver_status(cfg.device_id) if _driver_manager else None} for cfg in _registry.list_devices()]

@management_app.post("/api/devices", status_code=201)
async def add_device(body: DeviceAddRequest):
    if not _registry or not _driver_manager: raise HTTPException(503, "Not initialized")
    cfg = DeviceConfig(**body.model_dump())
    await _registry.save_device(cfg)
    if not await _driver_manager.add_device(cfg): raise HTTPException(400, f"Protocolo '{cfg.protocol}' não suportado")
    return {"device_id": cfg.device_id, "status": "added"}

@management_app.get("/api/devices/{device_id}")
async def get_device(device_id: str):
    if not _registry: raise HTTPException(503, "Not initialized")
    cfg = _registry.get_device(device_id)
    if not cfg: raise HTTPException(404, f"Device '{device_id}' não encontrado")
    return {**cfg.model_dump(), "runtime": _driver_manager.get_driver_status(device_id) if _driver_manager else None}

@management_app.patch("/api/devices/{device_id}/enable")
async def set_enabled(device_id: str, body: EnableRequest):
    if not _registry or not _driver_manager: raise HTTPException(503, "Not initialized")
    cfg = _registry.get_device(device_id)
    if not cfg: raise HTTPException(404, f"Device '{device_id}' não encontrado")
    cfg = cfg.model_copy(update={"enabled": body.enabled})
    await _registry.save_device(cfg)
    if body.enabled: await _driver_manager.add_device(cfg)
    else: await _driver_manager.remove_device(device_id)
    return {"device_id": device_id, "enabled": body.enabled}

@management_app.delete("/api/devices/{device_id}", status_code=204)
async def remove_device(device_id: str):
    if not _registry or not _driver_manager: raise HTTPException(503, "Not initialized")
    if not await _registry.remove_device(device_id): raise HTTPException(404, f"Device '{device_id}' não encontrado")
    await _driver_manager.remove_device(device_id)

@management_app.get("/api/devices/{device_id}/tags")
async def list_tags(device_id: str):
    if not _registry: raise HTTPException(503, "Not initialized")
    cfg = _registry.get_device(device_id)
    if not cfg: raise HTTPException(404, f"Device '{device_id}' não encontrado")
    return [t.model_dump() for t in cfg.tags]

@management_app.post("/api/discovery/scan")
async def discovery_scan(body: ScanRequest):
    scanner = _get_scanner(body.protocol)
    if not scanner: raise HTTPException(400, f"Protocolo de descoberta '{body.protocol}' não suportado")
    kwargs: dict[str, Any] = {}
    if body.protocol == "mqtt": kwargs["listen_seconds"] = body.listen_seconds
    elif body.protocol == "opcua": kwargs["max_depth"] = body.max_depth
    elif body.protocol == "modbus": kwargs.update({"slave_id": body.slave_id, "timeout": body.timeout})
    try:
        devices = await scanner.scan(body.target, **kwargs)
    except Exception as e:
        raise HTTPException(500, f"Scan falhou: {e}")
    return {"protocol": body.protocol, "target": body.target, "count": len(devices), "devices": [d.model_dump() for d in devices]}

def _get_scanner(protocol: str):
    try:
        if protocol == "opcua":
            from ..discovery.opcua import OpcUaScanner; return OpcUaScanner()
        if protocol == "modbus":
            from ..discovery.modbus import ModbusScanner; return ModbusScanner()
        if protocol == "mqtt":
            from ..discovery.mqtt import MqttScanner; return MqttScanner()
        if protocol == "enip":
            from ..discovery.enip import EtherNetIpScanner; return EtherNetIpScanner()
    except ImportError:
        pass
    return None

@management_app.get("/api/health")
async def health():
    if not _driver_manager: return {"status": "starting"}
    st = _driver_manager.status()
    connected = sum(1 for d in st["drivers"].values() if d.get("status") == "connected")
    total = len(st["drivers"])
    return {"status": "ok" if connected == total else ("degraded" if connected else "error"), "connected": connected, "total": total}

@management_app.get("/api/stats")
async def stats():
    if not _driver_manager: raise HTTPException(503, "Not initialized")
    return _driver_manager.status()
