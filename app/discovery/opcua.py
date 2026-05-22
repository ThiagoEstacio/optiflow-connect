"""OPC UA Discovery Scanner — browsa Address Space."""
from __future__ import annotations
import logging
from datetime import datetime, timezone
from .base import DiscoveryScanner
from ..core.registry import DeviceConfig, TagConfig
log = logging.getLogger(__name__)

class OpcUaScanner(DiscoveryScanner):
    PROTOCOL = "opcua"
    async def scan(self, target: str, max_depth: int = 2, **kwargs) -> list[DeviceConfig]:
        from asyncua import Client
        url = target if target.startswith("opc.tcp://") else f"opc.tcp://{target if ':' in target else target + ':4840'}"
        devices = []
        try:
            async with Client(url=url, timeout=10) as client:
                for node in await client.get_objects_node().get_children():
                    try:
                        name = (await node.read_display_name()).Text
                        if name == "Server": continue
                        tags = await _browse_variables(node, client, max_depth)
                        if tags:
                            devices.append(DeviceConfig(device_id=name, device_type="SENSOR", protocol="opcua", endpoint=url, tags=tags, discovery_source=f"opcua:{url}", created_at=datetime.now(timezone.utc)))
                    except Exception:
                        pass
        except Exception as e:
            log.error("opcua_scanner.error", extra={"error": str(e)})
        return devices

async def _browse_variables(node, client, depth: int) -> list[TagConfig]:
    from asyncua import ua
    tags = []
    if depth < 0: return tags
    try:
        for child in await node.get_children():
            nc = await child.read_node_class()
            if nc == ua.NodeClass.Variable:
                try:
                    name = (await child.read_display_name()).Text
                    dv   = await child.read_data_value()
                    float(dv.Value.Value)
                    tags.append(TagConfig(tag_id=name, address=child.nodeid.to_string()))
                except (TypeError, ValueError):
                    pass
            elif nc == ua.NodeClass.Object:
                tags.extend(await _browse_variables(child, client, depth - 1))
    except Exception:
        pass
    return tags
