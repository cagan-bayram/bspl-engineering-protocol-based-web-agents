from __future__ import annotations

import asyncio
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

import bspl
from bspl.adapter.core import Adapter
from bspl.adapter.http_adapter import HTTPEmitter, HTTPReceiver
from bspl.adapter.event import InitEvent  # IMPORTANT: to bootstrap initiators


# -------------------------
# Config (ENV-driven)
# -------------------------

PROTOCOL_DIR = Path(os.getenv("PROTOCOL_DIR", "protocols/reference"))
GENERATED_DIR = Path(os.getenv("BSPL_GENERATED_DIR", ".bspl_generated")).resolve()

API_PORT = int(os.getenv("API_PORT", "8001"))  # FastAPI REST
RECEIVER_PORT = int(os.getenv("RECEIVER_PORT", "9001"))  # aiohttp receiver
MESSAGES_PATH = os.getenv("MESSAGES_PATH", "/messages")

# Optional: default agent/role name for this process (so you don't pass role= every time)
AGENT_NAME = os.getenv("AGENT_NAME", "").strip()

BASE_URLS_RAW = os.getenv(
    "BASE_URLS",
    # IMPORTANT: these should point to RECEIVER ports, not API ports.
    "Buyer=http://127.0.0.1:9001,Seller=http://127.0.0.1:9002",
)


def normalize_protocol_name(protocol_name: str) -> str:
    protocol_name = (protocol_name or "").strip()
    if not protocol_name:
        return protocol_name
    return protocol_name[0].upper() + protocol_name[1:]


def parse_base_urls(raw: str) -> Dict[str, str]:
    raw = (raw or "").strip()
    if not raw:
        return {}
    out: Dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            raise ValueError(f"Invalid BASE_URLS entry (missing '='): {part}")
        k, v = part.split("=", 1)
        out[k.strip()] = v.strip().rstrip("/")
    return out


BASE_URLS = parse_base_urls(BASE_URLS_RAW)

print(f"Using PROTOCOL_DIR: {PROTOCOL_DIR}")
print(f"Using GENERATED_DIR: {GENERATED_DIR}")
print(f"Using API_PORT: {API_PORT}")
print(f"Using RECEIVER_PORT: {RECEIVER_PORT}")
print(f"Using MESSAGES_PATH: {MESSAGES_PATH}")
print(f"Using BASE_URLS: {BASE_URLS}")
print(f"Using AGENT_NAME: {AGENT_NAME or '(not set)'}")


# -------------------------
# FastAPI app
# -------------------------

app = FastAPI(
    title="BSPL Agent API",
    description="Serve BSPL specifications and enact protocols over HTTP",
)


# -------------------------
# In-memory state
# -------------------------

# One Adapter per (protocol, role) per process (KEEP THIS to preserve working behavior)
adapters: Dict[Tuple[str, str], Adapter] = {}

# Track that we've fully bootstrapped (receiver + update loop + init signal)
adapter_started: Dict[Tuple[str, str], bool] = {}

# enactment_id -> {"protocol":..., "role":..., "bindings": {...}}
# bindings are optional key/value constraints (e.g., {"ID": "1"})
enactments: Dict[str, Dict[str, Any]] = {}

_adapter_init_lock = asyncio.Lock()


# -------------------------
# Models
# -------------------------

class StartEnactmentResponse(BaseModel):
    protocol: str
    role: str
    enactment_id: str
    bindings: Dict[str, Any] = {}


class StartEnactmentBody(BaseModel):
    # Optional: keep a “focus” for this enactment (usually keys like ID / orderID)
    bindings: Dict[str, Any] = {}


class SendBody(BaseModel):
    payload: Dict[str, Any]


# -------------------------
# Helpers
# -------------------------

def safe_list(x: Any) -> List[Any]:
    if x is None:
        return []
    try:
        return list(x)
    except Exception:
        return []


def ensure_generated_importable() -> None:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    if str(GENERATED_DIR) not in sys.path:
        sys.path.insert(0, str(GENERATED_DIR))


def load_protocol(protocol_name: str):
    protocol_name = normalize_protocol_name(protocol_name)
    path = PROTOCOL_DIR / f"{protocol_name}.bspl"
    if not path.exists():
        raise HTTPException(
            status_code=404, detail=f"Protocol {protocol_name} not found at {path}"
        )
    spec = bspl.load_file(str(path))
    if protocol_name not in spec.protocols:
        raise HTTPException(
            status_code=400,
            detail=f"Protocol name '{protocol_name}' not found inside file. Available: {list(spec.protocols.keys())}",
        )
    return spec.protocols[protocol_name], spec


def export_module(protocol_name: str, spec):
    """
    spec.export() builds an in-memory python module and registers it.
    It does NOT write a file.
    """
    ensure_generated_importable()
    protocol_name = normalize_protocol_name(protocol_name)
    try:
        protocol_obj = spec.export(protocol_name)
        return protocol_obj.module
    except Exception as e:
        import traceback
        print("EXPORT ERROR:", e)
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Export failed: {e}")


async def _start_adapter_runtime(adapter: Adapter, key: Tuple[str, str], receiver: HTTPReceiver) -> None:
    """
    Bring the adapter to a 'running' state similar to Adapter.start(),
    but without taking over the whole process (FastAPI owns the loop).
    """
    if adapter_started.get(key):
        return

    # Ensure the event queue exists (Adapter.__init__ creates it, but be safe)
    if not hasattr(adapter, "events") or adapter.events is None:
        from asyncio.queues import Queue
        adapter.events = Queue()

    # Start update loop (processes ReceptionEvent / EmissionEvent into enabled_messages, etc.)
    adapter.running = True
    asyncio.create_task(adapter.update_loop())

    # Start receiver HTTP server
    asyncio.create_task(receiver.task(adapter))

    # Bootstrap initiators through the normal event mechanism
    await adapter.signal(InitEvent())

    adapter_started[key] = True

    print(f"Adapter runtime started for {key}")
    print(f"HTTPReceiver listening on http://{receiver.host}:{receiver.port}{receiver.path}")


def _resolve_role(role: Optional[str]) -> str:
    r = (role or "").strip()
    if r:
        return r
    if AGENT_NAME:
        return AGENT_NAME
    raise HTTPException(status_code=400, detail="Missing role. Provide ?role=... or set AGENT_NAME env var.")


async def ensure_adapter(protocol_name: str, role_name: str) -> Adapter:
    protocol_name = normalize_protocol_name(protocol_name)
    role_name = role_name.strip()

    key = (protocol_name, role_name)
    if key in adapters:
        return adapters[key]

    async with _adapter_init_lock:
        if key in adapters:
            return adapters[key]

        proto, spec = load_protocol(protocol_name)

        if role_name not in proto.roles:
            raise HTTPException(status_code=404, detail=f"Role {role_name} not found")

        mod = export_module(protocol_name, spec)

        # Receiver port derived from BASE_URLS if provided
        receiver_port = RECEIVER_PORT
        if role_name in BASE_URLS:
            from urllib.parse import urlparse
            parsed = urlparse(BASE_URLS[role_name])
            if parsed.port:
                receiver_port = parsed.port

        emitter = HTTPEmitter(base_urls=BASE_URLS, path=MESSAGES_PATH)
        receiver = HTTPReceiver(host="0.0.0.0", port=receiver_port, path=MESSAGES_PATH)

        # Adapter.send() indexes system["roles"] by Role objects -> agent name
        roles_map: Dict[Any, str] = {}

        # Map Role objects from the parsed protocol (these are often the ones schemas reference)
        for r in proto.roles.values():
            roles_map[r] = r.name

        # ALSO map Role objects from the exported module (these might be different instances)
        for r in proto.roles.values():
            if hasattr(mod, r.name):
                roles_map[getattr(mod, r.name)] = r.name

        systems = {
            "default": {
                "protocol": proto,
                "roles": roles_map,
            }
        }

        # Agents mapping for routing: agent name -> list of (host, port)
        agents: Dict[str, List[Tuple[str, int]]] = {}
        for r in proto.roles.values():
            if r.name in BASE_URLS:
                from urllib.parse import urlparse
                parsed = urlparse(BASE_URLS[r.name])
                agents[r.name] = [(parsed.hostname or "127.0.0.1", parsed.port or RECEIVER_PORT)]
            else:
                agents[r.name] = [("127.0.0.1", RECEIVER_PORT)]

        adapter = Adapter(
            name=role_name,
            systems=systems,
            agents=agents,
            emitter=emitter,
            receiver=receiver,
        )
        adapters[key] = adapter

        # Start receiver + update loop + init
        try:
            await _start_adapter_runtime(adapter, key, receiver)
            print(f"Adapter initialised for protocol '{protocol_name}' role '{role_name}'")
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Failed to start adapter runtime: {e}")

        return adapter


def require_enactment(protocol_name: str, enactment_id: str, role: str) -> Dict[str, Any]:
    protocol_name = normalize_protocol_name(protocol_name)
    role = role.strip()

    if enactment_id not in enactments:
        raise HTTPException(status_code=404, detail="Enactment not found")

    record = enactments[enactment_id]
    if record["protocol"] != protocol_name or record["role"] != role:
        raise HTTPException(status_code=400, detail="Enactment does not match protocol/role")

    return record


def _matches_bindings(payload: Dict[str, Any], bindings: Dict[str, Any]) -> bool:
    """
    Used to “focus” enabled messages on a particular enactment by key values.
    This is best-effort: if enabled messages don't have payloads yet, we won't filter.
    """
    if not bindings:
        return True
    if not isinstance(payload, dict):
        return True
    for k, v in bindings.items():
        if k in payload and payload.get(k) != v:
            return False
    return True


def _enabled_actions(adapter: Adapter, bindings: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert adapter.enabled_messages into a JSON list for the UI.
    Tries to filter by bindings when possible.
    """
    actions: List[Dict[str, Any]] = []

    # enabled_messages.messages() yields enabled “partial messages”
    enabled = list(adapter.enabled_messages.messages())

    for m in enabled:
        schema = getattr(m, "schema", None) or m
        schema_name = getattr(schema, "name", None)

        # Try to extract any currently known payload/bindings if present
        payload = getattr(m, "payload", None) if hasattr(m, "payload") else {}
        if isinstance(payload, dict) and not _matches_bindings(payload, bindings):
            continue

        actions.append(
            {
                "message": schema_name,
                "outs": safe_list(getattr(schema, "outs", [])),
                "ins": safe_list(getattr(schema, "ins", [])),
                # sendability hint for UI
                "known": payload if isinstance(payload, dict) else {},
            }
        )

    return actions


# -------------------------
# REST endpoints: health/state
# -------------------------

@app.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True}


@app.get("/state")
async def state(protocol: str, role: Optional[str] = None) -> Dict[str, Any]:
    """
    Minimal “agent state” endpoint:
    - enabled messages
    - protocol/role info
    - enactments in this process for that protocol/role
    """
    protocol = normalize_protocol_name(protocol)
    role = _resolve_role(role)

    adapter = await ensure_adapter(protocol, role)

    related_eids = [
        eid for eid, rec in enactments.items()
        if rec["protocol"] == protocol and rec["role"] == role
    ]

    return {
        "protocol": protocol,
        "role": role,
        "enactments": {eid: enactments[eid].get("bindings", {}) for eid in related_eids},
        "enabled": _enabled_actions(adapter, bindings={}),  # all enabled (UI can filter by enactment later)
    }


# -------------------------
# REST endpoints: protocols
# -------------------------

@app.get("/protocols", response_model=List[str])
def list_protocols() -> List[str]:
    if not PROTOCOL_DIR.exists():
        return []
    return [p.stem for p in PROTOCOL_DIR.glob("*.bspl")]


@app.get("/protocols/{protocol_name}")
def get_protocol(protocol_name: str) -> Dict[str, Any]:
    protocol_name = normalize_protocol_name(protocol_name)
    proto, _spec = load_protocol(protocol_name)

    summary = {
        "protocol": protocol_name,
        "roles": list(proto.roles.keys()),
        "messages": list(proto.messages.keys()),
        "keys": list(proto.keys),
        "keys_usage": {k: {"ins": [], "outs": [], "nils": []} for k in proto.keys},
    }

    for m_name, m in proto.messages.items():
        for k in proto.keys:
            if k in getattr(m, "ins", []):
                summary["keys_usage"][k]["ins"].append(m_name)
            if k in getattr(m, "outs", []):
                summary["keys_usage"][k]["outs"].append(m_name)
            if k in getattr(m, "nils", []):
                summary["keys_usage"][k]["nils"].append(m_name)

    return summary


@app.get("/protocols/{protocol_name}/roles/{role_name}")
def get_role_spec(protocol_name: str, role_name: str) -> Dict[str, Any]:
    protocol_name = normalize_protocol_name(protocol_name)
    proto, _spec = load_protocol(protocol_name)

    if role_name not in proto.roles:
        raise HTTPException(status_code=404, detail=f"Role {role_name} not found")

    messages = []
    for m_name, m in proto.messages.items():
        if m.sender and m.sender.name == role_name:
            direction = "send"
        elif m.recipients and any(r.name == role_name for r in safe_list(m.recipients)):
            direction = "receive"
        else:
            continue

        messages.append(
            {
                "name": m_name,
                "direction": direction,
                "ins": safe_list(getattr(m, "ins", [])),
                "outs": safe_list(getattr(m, "outs", [])),
                "nils": safe_list(getattr(m, "nils", [])),
                "parameters": [
                    {
                        "name": p_name,
                        "adornment": m.public_parameters[p_name].adornment,
                        "key": bool(m.public_parameters[p_name].key),
                    }
                    for p_name in m.public_parameters
                ],
            }
        )

    return {
        "role": role_name,
        "protocol": protocol_name,
        "keys": list(proto.keys),
        "messages": messages,
    }


# -------------------------
# REST endpoints: enactments/actions/send
# -------------------------

@app.post("/protocols/{protocol_name}/enactments", response_model=StartEnactmentResponse)
async def start_enactment(
    protocol_name: str,
    body: StartEnactmentBody,
    role: Optional[str] = Query(None, description="Role name for this agent instance (optional if AGENT_NAME set)"),
    enactment_id: Optional[str] = Query(None, description="Optional enactment id to share across agents"),
) -> StartEnactmentResponse:
    protocol_name = normalize_protocol_name(protocol_name)
    role = _resolve_role(role)

    await ensure_adapter(protocol_name, role)

    eid = enactment_id.strip() if enactment_id else str(uuid.uuid4())
    enactments[eid] = {"protocol": protocol_name, "role": role, "bindings": dict(body.bindings or {})}
    # Prime enabled messages with the enactment bindings (e.g., ID)
    try:
        adapter = adapters[(protocol_name, role)]  # ensure_adapter already created it
        adapter.compute_enabled(enactments[eid]["bindings"])
    except Exception as e:
        print("compute_enabled failed (ignored):", e)

    print(f"Started enactment {eid} for {protocol_name}/{role} bindings={enactments[eid]['bindings']}")

    return StartEnactmentResponse(protocol=protocol_name, role=role, enactment_id=eid, bindings=enactments[eid]["bindings"])


@app.get("/protocols/{protocol_name}/enactments/{enactment_id}/actions")
async def get_enabled_actions(protocol_name: str, enactment_id: str, role: Optional[str] = None) -> Dict[str, Any]:
    protocol_name = normalize_protocol_name(protocol_name)
    role = _resolve_role(role)

    record = require_enactment(protocol_name, enactment_id, role)
    bindings = record.get("bindings", {}) or {}

    adapter = await ensure_adapter(protocol_name, role)

    # Ensure enabled messages reflect current bindings
    try:
        adapter.compute_enabled(bindings)
    except Exception as e:
        print("compute_enabled failed (ignored):", e)

    actions = _enabled_actions(adapter, bindings=bindings)

    return {
        "protocol": protocol_name,
        "role": role,
        "enactment_id": enactment_id,
        "bindings": bindings,
        "actions": actions,
    }


@app.post("/protocols/{protocol_name}/enactments/{enactment_id}/send/{message_name}")
async def send_message(
    protocol_name: str,
    enactment_id: str,
    message_name: str,
    body: SendBody,
    role: Optional[str] = None,
) -> Dict[str, Any]:
    protocol_name = normalize_protocol_name(protocol_name)
    role = _resolve_role(role)

    record = require_enactment(protocol_name, enactment_id, role)
    bindings = record.get("bindings", {}) or {}

    adapter = await ensure_adapter(protocol_name, role)
    proto, _spec = load_protocol(protocol_name)

    if message_name not in proto.messages:
        raise HTTPException(status_code=404, detail=f"Message '{message_name}' not found in protocol")

    message_schema = proto.messages[message_name]

    # Optional: enforce that user payload doesn't contradict enactment bindings
    for k, v in bindings.items():
        if k in body.payload and body.payload[k] != v:
            raise HTTPException(status_code=400, detail=f"Payload contradicts enactment binding {k}={v}")

    from bspl.adapter.message import Message

    try:
        message = Message(
            schema=message_schema,
            payload=body.payload,
            meta={"enactment": enactment_id},
            acknowledged=False,
            dest=None,
            adapter=adapter,
            system="default",
        )
        await adapter.send(message)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=f"adapter.send() failed: {e}")

    return {"status": "sent", "message": {"name": message_name, "payload": body.payload}}


# -------------------------
# Main
# -------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=API_PORT, reload=False)
