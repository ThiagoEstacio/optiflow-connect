"""DeviceRegistry — persistência SQLite de configurações de conectividade.

Responsabilidade única: 'como conectar a este dispositivo e quais tags ler.'
NÃO é responsabilidade: hierarquia de ativos → OptiFlow Context
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import aiosqlite
from pydantic import BaseModel, Field

log = logging.getLogger(__name__)

_DDL = """
CREATE TABLE IF NOT EXISTS devices (
    device_id       TEXT PRIMARY KEY,
    device_type     TEXT NOT NULL,
    protocol        TEXT NOT NULL,
    endpoint        TEXT NOT NULL,
    enabled         INTEGER NOT NULL DEFAULT 1,
    poll_interval_s REAL    NOT NULL DEFAULT 2.0,
    options         TEXT    NOT NULL DEFAULT '{}',
    discovery_source TEXT,
    created_at      TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS tags (
    device_id   TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
    tag_id      TEXT NOT NULL,
    address     TEXT NOT NULL,
    unit        TEXT,
    scale       REAL NOT NULL DEFAULT 1.0,
    offset      REAL NOT NULL DEFAULT 0.0,
    writable    INTEGER NOT NULL DEFAULT 0,
    options     TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (device_id, tag_id)
);
"""


class TagConfig(BaseModel):
    tag_id:   str
    address:  str
    unit:     str | None = None
    scale:    float      = 1.0
    offset:   float      = 0.0
    writable: bool       = False
    options:  dict[str, Any] = Field(default_factory=dict)


class DeviceConfig(BaseModel):
    device_id:        str
    device_type:      str
    protocol:         str
    endpoint:         str
    enabled:          bool  = True
    poll_interval_s:  float = 2.0
    tags:             list[TagConfig] = Field(default_factory=list)
    options:          dict[str, Any]  = Field(default_factory=dict)
    discovery_source: str | None      = None
    created_at:       datetime        = Field(default_factory=lambda: datetime.now(timezone.utc))


class DeviceRegistry:
    def __init__(self, db_path: str = "/app/data/devices.db"):
        self._path    = db_path
        self._devices: dict[str, DeviceConfig] = {}

    async def load(self) -> None:
        async with aiosqlite.connect(self._path) as db:
            db.row_factory = aiosqlite.Row
            await db.executescript(_DDL)
            await db.commit()
            devices_rows = await db.execute_fetchall("SELECT * FROM devices")
            for row in devices_rows:
                tags_rows = await db.execute_fetchall(
                    "SELECT * FROM tags WHERE device_id = ?", (row["device_id"],)
                )
                tags = [
                    TagConfig(
                        tag_id=t["tag_id"], address=t["address"], unit=t["unit"],
                        scale=t["scale"], offset=t["offset"], writable=bool(t["writable"]),
                        options=json.loads(t["options"]),
                    )
                    for t in tags_rows
                ]
                self._devices[row["device_id"]] = DeviceConfig(
                    device_id=row["device_id"], device_type=row["device_type"],
                    protocol=row["protocol"], endpoint=row["endpoint"],
                    enabled=bool(row["enabled"]), poll_interval_s=row["poll_interval_s"],
                    tags=tags, options=json.loads(row["options"]),
                    discovery_source=row["discovery_source"],
                    created_at=datetime.fromisoformat(row["created_at"]),
                )
        log.info("registry.loaded", extra={"count": len(self._devices), "db": self._path})

    async def save_device(self, cfg: DeviceConfig) -> None:
        async with aiosqlite.connect(self._path) as db:
            await db.execute(
                """
                INSERT INTO devices
                  (device_id, device_type, protocol, endpoint, enabled,
                   poll_interval_s, options, discovery_source, created_at)
                VALUES (?,?,?,?,?,?,?,?,?)
                ON CONFLICT(device_id) DO UPDATE SET
                  device_type=excluded.device_type, protocol=excluded.protocol,
                  endpoint=excluded.endpoint, enabled=excluded.enabled,
                  poll_interval_s=excluded.poll_interval_s, options=excluded.options,
                  discovery_source=excluded.discovery_source
                """,
                (cfg.device_id, cfg.device_type, cfg.protocol, cfg.endpoint,
                 int(cfg.enabled), cfg.poll_interval_s, json.dumps(cfg.options),
                 cfg.discovery_source, cfg.created_at.isoformat()),
            )
            await db.execute("DELETE FROM tags WHERE device_id = ?", (cfg.device_id,))
            for tag in cfg.tags:
                await db.execute(
                    "INSERT INTO tags (device_id, tag_id, address, unit, scale, offset, writable, options) VALUES (?,?,?,?,?,?,?,?)",
                    (cfg.device_id, tag.tag_id, tag.address, tag.unit, tag.scale, tag.offset, int(tag.writable), json.dumps(tag.options)),
                )
            await db.commit()
        self._devices[cfg.device_id] = cfg

    async def remove_device(self, device_id: str) -> bool:
        if device_id not in self._devices:
            return False
        async with aiosqlite.connect(self._path) as db:
            await db.execute("DELETE FROM devices WHERE device_id = ?", (device_id,))
            await db.commit()
        del self._devices[device_id]
        return True

    def list_devices(self) -> list[DeviceConfig]:
        return list(self._devices.values())

    def get_device(self, device_id: str) -> DeviceConfig | None:
        return self._devices.get(device_id)

    def __len__(self) -> int:
        return len(self._devices)
