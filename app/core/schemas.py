"""Contrato de mensagens do barramento Gateway ↔ OPERA."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

Quality = Literal["good", "bad", "uncertain", "stale"]

class GatewayReading(BaseModel):
    msg_id:     str      = Field(default_factory=lambda: str(uuid4()))
    schema_ver: str      = "1.0"
    ts:         datetime
    ingested_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    gateway_id: str
    site_id:    str      = "BR/SP/SAO"
    device_id:  str
    device_type: str
    tag_id:     str
    value:      float
    quality:    Quality  = "good"
    unit:       str | None = None
    protocol:   str      = "opcua"
    raw:        dict[str, Any] | None = None

    def to_stream_entry(self) -> dict[str, str]:
        import json
        return {
            "msg_id":      self.msg_id,
            "schema_ver":  self.schema_ver,
            "ts":          self.ts.isoformat(),
            "ingested_at": self.ingested_at.isoformat(),
            "gateway_id":  self.gateway_id,
            "site_id":     self.site_id,
            "device_id":   self.device_id,
            "device_type": self.device_type,
            "tag_id":      self.tag_id,
            "value":       str(self.value),
            "quality":     self.quality,
            "unit":        self.unit or "",
            "protocol":    self.protocol,
            "raw":         json.dumps(self.raw) if self.raw else "{}",
        }

    @classmethod
    def from_stream_entry(cls, entry: dict[str, str]) -> "GatewayReading":
        import json
        raw = entry.get("raw", "{}")
        return cls(
            msg_id=entry["msg_id"],
            schema_ver=entry.get("schema_ver", "1.0"),
            ts=datetime.fromisoformat(entry["ts"]),
            ingested_at=datetime.fromisoformat(entry["ingested_at"]),
            gateway_id=entry["gateway_id"],
            site_id=entry.get("site_id", "BR/SP/SAO"),
            device_id=entry["device_id"],
            device_type=entry["device_type"],
            tag_id=entry["tag_id"],
            value=float(entry["value"]),
            quality=entry.get("quality", "good"),  # type: ignore
            unit=entry.get("unit") or None,
            protocol=entry.get("protocol", "opcua"),
            raw=json.loads(raw) if raw and raw != "{}" else None,
        )


CommandAction = Literal[
    "set_setpoint", "pump_on", "pump_off", "set_mode", "set_param",
]

class OperaCommand(BaseModel):
    msg_id:        str      = Field(default_factory=lambda: str(uuid4()))
    schema_ver:    str      = "1.0"
    ts:            datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    proposal_id:   int | None = None
    device_id:     str
    device_type:   str
    action:        CommandAction
    params:        dict[str, Any]
    actor:         str
    correlation_id: str = Field(default_factory=lambda: str(uuid4()))
    ttl_s:         int  = 30

    def to_stream_entry(self) -> dict[str, str]:
        import json
        return {
            "msg_id":         self.msg_id,
            "schema_ver":     self.schema_ver,
            "ts":             self.ts.isoformat(),
            "proposal_id":    str(self.proposal_id or ""),
            "device_id":      self.device_id,
            "device_type":    self.device_type,
            "action":         self.action,
            "params":         json.dumps(self.params),
            "actor":          self.actor,
            "correlation_id": self.correlation_id,
            "ttl_s":          str(self.ttl_s),
        }

    @classmethod
    def from_stream_entry(cls, entry: dict[str, str]) -> "OperaCommand":
        import json
        pid = entry.get("proposal_id", "")
        return cls(
            msg_id=entry["msg_id"],
            schema_ver=entry.get("schema_ver", "1.0"),
            ts=datetime.fromisoformat(entry["ts"]),
            proposal_id=int(pid) if pid else None,
            device_id=entry["device_id"],
            device_type=entry["device_type"],
            action=entry["action"],  # type: ignore
            params=json.loads(entry["params"]),
            actor=entry["actor"],
            correlation_id=entry.get("correlation_id", str(uuid4())),
            ttl_s=int(entry.get("ttl_s", "30")),
        )


class GatewayAck(BaseModel):
    msg_id:         str      = Field(default_factory=lambda: str(uuid4()))
    ts:             datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    command_msg_id: str
    proposal_id:    int | None = None
    device_id:      str
    action:         str
    success:        bool
    error:          str | None = None
    applied_value:  float | None = None
    latency_ms:     float | None = None

    def to_stream_entry(self) -> dict[str, str]:
        return {
            "msg_id":         self.msg_id,
            "ts":             self.ts.isoformat(),
            "command_msg_id": self.command_msg_id,
            "proposal_id":    str(self.proposal_id or ""),
            "device_id":      self.device_id,
            "action":         self.action,
            "success":        "1" if self.success else "0",
            "error":          self.error or "",
            "applied_value":  str(self.applied_value) if self.applied_value is not None else "",
            "latency_ms":     str(self.latency_ms) if self.latency_ms is not None else "",
        }


STREAM_READINGS  = "gateway:readings"
STREAM_COMMANDS  = "opera:commands"
STREAM_ACKS      = "gateway:acks"

GROUP_OPERA      = "opera-cognitive"
GROUP_INFLUX     = "influxdb-writer"
GROUP_ALERTS     = "alerting"
GROUP_GATEWAY    = "gateway-executor"
