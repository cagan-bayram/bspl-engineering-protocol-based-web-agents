from __future__ import annotations

import asyncio
import os
import uuid
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel

import bspl
from bspl.adapter.core import Adapter
from bspl.adapter.http_adapter import HTTPEmitter
from bspl.adapter.event import InitEvent
from bspl.adapter.message import Message

# ─────────────────────────────────────────────────────────────────
# Static config (env vars — only node-level, not agent-level)
# ─────────────────────────────────────────────────────────────────
PROTOCOL_DIR = Path(os.getenv("PROTOCOL_DIR", "protocols/reference"))
API_PORT     = int(os.getenv("API_PORT", "8001"))
AGENT_NAME   = os.getenv("AGENT_NAME", "").strip()
PUBLIC_HOST  = os.getenv("PUBLIC_HOST", "127.0.0.1")

STORAGE_DIR   = Path("storage")
REGISTRY_FILE = STORAGE_DIR / "agent_registry.json"
STATE_FILE    = STORAGE_DIR / "system_state.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s: %(message)s")
logger = logging.getLogger("sliq")

# ─────────────────────────────────────────────────────────────────
# Runtime state
# ─────────────────────────────────────────────────────────────────
AGENT_ID: str = ""   # Set at startup from registry

# active_agents[mas_id] = {
#   "adapter":    Adapter,
#   "config":     { mas_id, protocol_name, role, topology: Dict[str,str] },
#   "enactments": { eid: { protocol, role, bindings } }
# }
active_agents: Dict[str, dict] = {}


# ─────────────────────────────────────────────────────────────────
# NoOpReceiver — we handle inbound messages via FastAPI route
# ─────────────────────────────────────────────────────────────────
class NoOpReceiver:
    async def task(self, adapter):
        pass  # FastAPI POST /{agent_id}/messages does the receiving

    async def stop(self):
        pass


# ─────────────────────────────────────────────────────────────────
# Utility helpers
# ─────────────────────────────────────────────────────────────────
def safe_list(x):
    try:
        return list(x)
    except Exception:
        return []


def normalize_name(n: str) -> str:
    return (n[0].upper() + n[1:]) if n else n


def load_protocol(protocol_name: str):
    pn = normalize_name(protocol_name)
    path = PROTOCOL_DIR / f"{pn.lower()}.bspl"
    if not path.exists():
        path = PROTOCOL_DIR / f"{pn}.bspl"
    if not path.exists():
        raise HTTPException(404, f"Protocol '{pn}' not found in {PROTOCOL_DIR}")
    spec = bspl.load_file(str(path))
    return spec.protocols[pn], spec


def resolve_agent_id(name: str) -> str:
    """Return persistent UUID for agent name; create it if first use."""
    STORAGE_DIR.mkdir(exist_ok=True)
    registry: Dict[str, str] = {}
    if REGISTRY_FILE.exists():
        try:
            registry = json.loads(REGISTRY_FILE.read_text())
        except Exception:
            pass
    key = name.strip().lower()
    if key not in registry:
        registry[key] = str(uuid.uuid4())
        REGISTRY_FILE.write_text(json.dumps(registry, indent=2))
        logger.info(f"New agent '{name}' registered → {registry[key]}")
    else:
        logger.info(f"Agent '{name}' restored   → {registry[key]}")
    return registry[key]


# ─────────────────────────────────────────────────────────────────
# Core join logic (also called by StateManager.load for recovery)
# ─────────────────────────────────────────────────────────────────
class JoinBody(BaseModel):
    mas_id:        str
    protocol_name: str
    role:          str
    topology:      Dict[str, str]   # { other_role: "http://host:port/{their_uuid}" }


async def _do_join(body: JoinBody) -> dict:
    """Create Adapter + register in active_agents. Idempotent."""
    if body.mas_id in active_agents:
        return active_agents[body.mas_id]

    proto, _ = load_protocol(body.protocol_name)
    protocol_name = normalize_name(body.protocol_name)
    topology = body.topology

    # HTTPEmitter — sends to peer base URLs + "/messages?mas_id=..."
    # This ensures the receiver routes the message to the correct MAS session!
    emitter = HTTPEmitter(base_urls=topology, path=f"/messages?mas_id={body.mas_id}")

    # roles_map: Role object → agent-name string
    # We use the role name as agent name (one agent per role in our model)
    roles_map: Dict = {r: r.name for r in proto.roles.values()}

    # agents dict: agent_name → [(host, port)] endpoint list
    agents: Dict[str, list] = {}
    for r in proto.roles.values():
        if r.name == body.role:
            # Our own address — FastAPI port (receiver is NoOp, so value only
            # needs to be valid for the Adapter agents dict structure)
            agents[r.name] = [("127.0.0.1", API_PORT)]
        elif r.name in topology:
            parsed = urlparse(topology[r.name])
            host = parsed.hostname or "127.0.0.1"
            port = parsed.port or 80
            agents[r.name] = [(host, port)]
        else:
            agents[r.name] = [("127.0.0.1", API_PORT)]

    systems = {"default": {"protocol": proto, "roles": roles_map}}

    adapter = Adapter(
        name=body.role,
        systems=systems,
        agents=agents,
        emitter=emitter,
        receiver=NoOpReceiver(),
    )

    # Start adapter background event loop
    adapter.events  = asyncio.Queue()
    adapter.running = True
    asyncio.create_task(adapter.update_loop())
    await adapter.signal(InitEvent())

    slot = {
        "adapter":    adapter,
        "config":     {
            "mas_id":        body.mas_id,
            "protocol_name": protocol_name,
            "role":          body.role,
            "topology":      topology,
        },
        "enactments": {},
    }
    active_agents[body.mas_id] = slot
    logger.info(f"Joined MAS '{body.mas_id}' as {body.role} in {protocol_name}")
    return slot


# ─────────────────────────────────────────────────────────────────
# State persistence
# ─────────────────────────────────────────────────────────────────
class StateManager:

    @staticmethod
    def save():
        STORAGE_DIR.mkdir(exist_ok=True)
        data: Dict[str, dict] = {}
        for mas_id, slot in active_agents.items():
            history_items = []
            adapter = slot["adapter"]
            for msg in adapter.history.messages():
                try:
                    schema = getattr(msg, "schema", None)
                    if schema is None:
                        continue
                    raw_payload = getattr(msg, "payload", {}) or {}
                    history_items.append({
                        "schema":  schema.qualified_name,
                        "payload": {str(getattr(k, "name", k)): v for k, v in raw_payload.items()},
                        "meta":    {k: str(v) for k, v in (getattr(msg, "meta", {}) or {}).items()},
                        "system":  getattr(msg, "system", "default"),
                    })
                except Exception:
                    pass

            data[mas_id] = {
                "config":     slot["config"],
                "enactments": slot.get("enactments", {}),
                "history":    history_items,
            }

        # Merge with any other agents' data already in the file
        existing: Dict = {}
        if STATE_FILE.exists():
            try:
                existing = json.loads(STATE_FILE.read_text())
            except Exception:
                pass
        existing[AGENT_ID] = data

        try:
            STATE_FILE.write_text(json.dumps(existing, indent=2, default=str))
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    @staticmethod
    async def load():
        if not STATE_FILE.exists():
            return
        try:
            all_data = json.loads(STATE_FILE.read_text())
            agent_data: Dict = all_data.get(AGENT_ID, {})

            for mas_id, slot_data in agent_data.items():
                cfg = slot_data.get("config", {})
                try:
                    body = JoinBody(
                        mas_id=cfg["mas_id"],
                        protocol_name=cfg["protocol_name"],
                        role=cfg["role"],
                        topology=cfg.get("topology", {}),
                    )
                    await _do_join(body)
                except Exception as e:
                    logger.warning(f"Could not restore MAS '{mas_id}': {e}")
                    continue

                # Restore enactment records
                active_agents[mas_id]["enactments"] = slot_data.get("enactments", {})

                # Replay message history into adapter
                adapter = active_agents[mas_id]["adapter"]
                proto, _ = load_protocol(cfg["protocol_name"])
                for item in slot_data.get("history", []):
                    try:
                        schema_key = item.get("schema", "")
                        # qualified_name is "ProtocolName.MessageName"
                        msg_name = schema_key.split(".")[-1]
                        schema = proto.messages.get(msg_name)
                        if schema is None:
                            continue
                        msg = Message(
                            schema=schema,
                            payload=item.get("payload", {}),
                            meta=item.get("meta", {}),
                            system=item.get("system", "default"),
                        )
                        if not adapter.history.context(msg).find(schema):
                            adapter.history.add(msg)
                    except Exception:
                        pass

            logger.info(f"State restored: {len(agent_data)} MAS session(s)")
        except Exception as e:
            logger.error(f"Failed to load state: {e}")


# ─────────────────────────────────────────────────────────────────
# FastAPI app
# ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global AGENT_ID
    if AGENT_NAME:
        AGENT_ID = resolve_agent_id(AGENT_NAME)
    else:
        # Unnamed node — transient UUID (no persistence possible)
        AGENT_ID = str(uuid.uuid4())
        logger.warning("AGENT_NAME not set; node identity is transient this session")
    logger.info(f"Node identity: '{AGENT_NAME}' → {AGENT_ID}  (API on :{API_PORT})")
    await StateManager.load()
    yield
    StateManager.save()


app = FastAPI(title="Sliq BSPL Node", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────────
# UI routes — serve dashboard.html for both root and agent views
# ─────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return FileResponse("dashboard.html")


@app.get("/{agent_id}/dashboard")
async def agent_dashboard(agent_id: str):
    if agent_id != AGENT_ID:
        raise HTTPException(404, "Unknown agent ID")
    return FileResponse("dashboard.html")


# ─────────────────────────────────────────────────────────────────
# Identity & node status
# ─────────────────────────────────────────────────────────────────
@app.get("/api/identity")
def get_identity():
    return {
        "agent_name": AGENT_NAME,
        "agent_id":   AGENT_ID,
        "api_port":   API_PORT,
        "public_host": PUBLIC_HOST,
        "webhook_base": f"http://{PUBLIC_HOST}:{API_PORT}/{AGENT_ID}",
    }


@app.get("/api/agents")
def get_agents():
    sessions = []
    for mas_id, slot in active_agents.items():
        cfg = slot["config"]
        sessions.append({
            "mas_id":          mas_id,
            "protocol":        cfg["protocol_name"],
            "role":            cfg["role"],
            "enactment_ids":   list(slot["enactments"].keys()),
            "dashboard_url":   f"/{AGENT_ID}/dashboard",
        })
    return {
        "agent_id":    AGENT_ID,
        "agent_name":  AGENT_NAME,
        "mas_sessions": sessions,
    }


# ─────────────────────────────────────────────────────────────────
# Protocol discovery
# ─────────────────────────────────────────────────────────────────
@app.get("/protocols")
def list_protocols():
    names: List[str] = []
    if PROTOCOL_DIR.exists():
        for f in sorted(PROTOCOL_DIR.glob("*.bspl")):
            names.append(normalize_name(f.stem))
    return {"protocols": names}


@app.get("/protocols/{protocol_name}")
def get_protocol_spec(protocol_name: str):
    proto, _ = load_protocol(protocol_name)

    messages = []
    for msg in proto.messages.values():
        ins_list, outs_list = [], []

        for ref in safe_list(getattr(msg, "ins", [])):
            p = ref.param if hasattr(ref, "param") else ref
            p_name = str(getattr(p, "name", p)).strip()
            param_obj = proto.public_parameters.get(p_name) if hasattr(proto, "public_parameters") else None
            is_key = (param_obj and getattr(param_obj, "key", None) == "key")
            ins_list.append({
                "name": p_name,
                "key":  is_key,
            })

        for ref in safe_list(getattr(msg, "outs", [])):
            p = ref.param if hasattr(ref, "param") else ref
            p_name = str(getattr(p, "name", p)).strip()
            param_obj = proto.public_parameters.get(p_name) if hasattr(proto, "public_parameters") else None
            is_key = (param_obj and getattr(param_obj, "key", None) == "key")
            outs_list.append({
                "name": p_name,
                "key":  is_key,
            })

        messages.append({
            "name":       msg.name,
            "sender":     str(getattr(msg.sender, "name", msg.sender)),
            "recipients": [str(getattr(r, "name", r)) for r in safe_list(msg.recipients)],
            "ins":        ins_list,
            "outs":       outs_list,
        })

    return {
        "name":     normalize_name(protocol_name),
        "roles":    list(proto.roles.keys()),
        "messages": messages,
    }


# ─────────────────────────────────────────────────────────────────
# Join a MAS
# ─────────────────────────────────────────────────────────────────
@app.post("/api/join")
async def api_join(body: JoinBody, background_tasks: BackgroundTasks):
    if body.mas_id in active_agents:
        raise HTTPException(409, f"Already joined MAS '{body.mas_id}' in this session")
    await _do_join(body)
    background_tasks.add_task(StateManager.save)
    return {
        "status":        "joined",
        "mas_id":        body.mas_id,
        "role":          body.role,
        "dashboard_url": f"/{AGENT_ID}/dashboard",
    }


# ─────────────────────────────────────────────────────────────────
# Inbound peer message webhook
# ─────────────────────────────────────────────────────────────────
@app.post("/{agent_id}/messages")
async def receive_message(agent_id: str, request: Request, background_tasks: BackgroundTasks, mas_id: Optional[str] = None):
    if agent_id != AGENT_ID:
        raise HTTPException(404, "Unknown agent")

    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON body")

    schema_key = data.get("schema", "")
    target_slot: Optional[dict] = None

    # Precise routing if the sender provided the MAS ID in the webhook URL
    if mas_id and mas_id in active_agents:
        slot = active_agents[mas_id]
        if schema_key in slot["adapter"].messages:
            target_slot = slot

    # Fallback to scanning all sessions if no/invalid MAS ID provided
    if target_slot is None:
        for slot in active_agents.values():
            if schema_key in slot["adapter"].messages:
                target_slot = slot
                break

    if target_slot is None:
        raise HTTPException(404, f"No active adapter handles schema '{schema_key}'")

    # ── Invitation model ─────────────────────────────────────────────
    # If this message carries an enactment ID we don't know about,
    # auto-create the record so the receiver's dashboard shows it
    # without requiring a manual "Start Enactment" click.
    inbound_eid = data.get("meta", {}).get("enactment")
    inbound_payload = data.get("payload", {})
    if inbound_eid:
        enactments = target_slot["enactments"]
        if inbound_eid not in enactments:
            cfg = target_slot["config"]
            enactments[inbound_eid] = {
                "protocol": cfg["protocol_name"],
                "role":     cfg["role"],
                "bindings": dict(inbound_payload),
            }
            logger.info(
                f"[Invitation] Auto-created enactment '{inbound_eid}' "
                f"in MAS '{cfg['mas_id']}' (triggered by incoming {schema_key})"
            )
            background_tasks.add_task(StateManager.save)

    await target_slot["adapter"].receive(data)
    background_tasks.add_task(StateManager.save)
    return {"status": "ok"}


# ─────────────────────────────────────────────────────────────────
# Start a protocol enactment
# ─────────────────────────────────────────────────────────────────
class EnactmentBody(BaseModel):
    enactment_id: Optional[str] = None
    bindings:     Dict[str, Any] = {}


@app.post("/{agent_id}/enactments")
async def start_enactment(
    agent_id: str,
    body: EnactmentBody,
    background_tasks: BackgroundTasks,
    mas_id: str = Query(..., description="Which MAS session to enact under"),
):
    if agent_id != AGENT_ID:
        raise HTTPException(404, "Unknown agent")
    if mas_id not in active_agents:
        raise HTTPException(404, f"Not joined MAS '{mas_id}'")

    slot = active_agents[mas_id]
    eid  = (body.enactment_id or "").strip() or str(uuid.uuid4())

    if eid in slot["enactments"]:
        return {
            "enactment_id": eid,
            "mas_id":       mas_id,
            "role":         slot["config"]["role"],
            "status":       "already_exists",
        }

    slot["enactments"][eid] = {
        "protocol": slot["config"]["protocol_name"],
        "role":     slot["config"]["role"],
        "bindings": dict(body.bindings),
    }
    background_tasks.add_task(StateManager.save)
    return {
        "enactment_id": eid,
        "mas_id":       mas_id,
        "role":         slot["config"]["role"],
        "status":       "created",
    }


# ─────────────────────────────────────────────────────────────────
# Get enabled actions for an enactment
# ─────────────────────────────────────────────────────────────────
@app.get("/{agent_id}/enactments/{enactment_id}/actions")
async def get_actions(
    agent_id:     str,
    enactment_id: str,
    mas_id:       str = Query(...),
):
    if agent_id != AGENT_ID:
        raise HTTPException(404, "Unknown agent")
    if mas_id not in active_agents:
        raise HTTPException(404, f"Not joined MAS '{mas_id}'")

    slot = active_agents[mas_id]
    adapter = slot["adapter"]
    enactments = slot["enactments"]

    if enactment_id not in enactments:
        raise HTTPException(404, "Enactment not found")

    record   = enactments[enactment_id]
    bindings = record["bindings"]
    updated  = False

    proto = None
    protocol_keys = set()
    try:
        proto, _ = load_protocol(slot["config"]["protocol_name"])
        if hasattr(proto, "public_parameters"):
            for p_name, param_obj in proto.public_parameters.items():
                if getattr(param_obj, "key", None) == "key":
                    protocol_keys.add(p_name.strip())
    except Exception:
        pass

    # ── 1. Sync bindings from history ──────────────────────────────
    for m in list(adapter.history.messages()):
        raw_payload = getattr(m, "payload", {}) or {}
        payload = {str(getattr(k, "name", k)).strip(): v for k, v in raw_payload.items()}
        meta    = getattr(m, "meta", {}) or {}

        is_mine = meta.get("enactment") == enactment_id
        if not is_mine and payload:
            if enactment_id in payload.values():
                is_mine = True
            elif bindings and protocol_keys:
                shared = set(payload.keys()) & set(bindings.keys()) & protocol_keys
                if shared and all(payload[k] == bindings[k] for k in shared):
                    is_mine = True

        if is_mine:
            msg_schema  = getattr(m, "schema", None)
            valid_params = set()
            if msg_schema:
                for ref in safe_list(getattr(msg_schema, "ins", [])) + safe_list(getattr(msg_schema, "outs", [])):
                    p = ref.param if hasattr(ref, "param") else ref
                    valid_params.add(str(getattr(p, "name", p)).strip())
            for k, v in payload.items():
                if valid_params and k not in valid_params:
                    continue
                if k not in bindings or bindings[k] != v:
                    bindings[k] = v
                    updated = True

    if updated:
        StateManager.save()

    # ── 2. Build actions from enabled_messages ──────────────────────
    actions = []
    for m in adapter.enabled_messages.messages():
        schema = getattr(m, "schema", m)

        # Causality cull — skip if already sent in this enactment
        already_enacted = False
        for hm in adapter.history.messages():
            h_schema  = getattr(hm, "schema", None)
            h_payload = {str(getattr(k, "name", k)).strip(): v for k, v in (getattr(hm, "payload", {}) or {}).items()}
            h_meta    = getattr(hm, "meta", {}) or {}
            h_mine = h_meta.get("enactment") == enactment_id
            if not h_mine and h_payload:
                if enactment_id in h_payload.values():
                    h_mine = True
                elif bindings and protocol_keys:
                    sc = set(h_payload.keys()) & set(bindings.keys()) & protocol_keys
                    if sc and all(h_payload[k] == bindings[k] for k in sc):
                        h_mine = True
            if h_schema == schema and h_mine:
                already_enacted = True
                break
        if already_enacted:
            continue

        known_ins = {
            getattr(p, "name", str(p)): bindings[getattr(p, "name", str(p))]
            for p in safe_list(getattr(schema, "ins", []))
            if getattr(p, "name", str(p)) in bindings
        }

        outs_data = []
        for x in safe_list(getattr(schema, "outs", [])):
            p      = x.param if hasattr(x, "param") else x
            p_name = str(getattr(p, "name", x)).strip()
            is_key = p_name in protocol_keys
            outs_data.append({"name": p_name, "is_key": is_key})

        actions.append({
            "message":    schema.name,
            "sender":     str(getattr(schema.sender, "name", schema.sender)),
            "recipients": [str(getattr(r, "name", r)) for r in safe_list(schema.recipients)],
            "ins":        [getattr(x, "name", str(x)) for x in safe_list(getattr(schema, "ins", []))],
            "outs":       outs_data,
            "known":      known_ins,
        })

    return {
        "enactment_id": enactment_id,
        "mas_id":       mas_id,
        "role":         record["role"],
        "bindings":     bindings,
        "actions":      actions,
    }


# ─────────────────────────────────────────────────────────────────
# Send a message
# ─────────────────────────────────────────────────────────────────
@app.post("/{agent_id}/enactments/{enactment_id}/send/{message_name}")
async def send_message(
    agent_id:     str,
    enactment_id: str,
    message_name: str,
    request:      Request,
    background_tasks: BackgroundTasks,
    mas_id:       str = Query(...),
):
    if agent_id != AGENT_ID:
        raise HTTPException(404, "Unknown agent")
    if mas_id not in active_agents:
        raise HTTPException(404, f"Not joined MAS '{mas_id}'")

    slot = active_agents[mas_id]
    adapter    = slot["adapter"]
    enactments = slot["enactments"]
    topology   = slot["config"]["topology"]

    if enactment_id not in enactments:
        raise HTTPException(404, "Enactment not found")

    record   = enactments[enactment_id]
    bindings = record["bindings"]

    body        = await request.json()
    req_payload = body.get("payload", {})

    # Find schema in enabled messages
    schema = next(
        (getattr(m, "schema", m) for m in adapter.enabled_messages.messages()
         if getattr(m, "schema", m).name == message_name),
        None,
    )
    if schema is None:
        raise HTTPException(400, f"Message '{message_name}' is not currently enabled")

    proto = None
    try:
        proto, _ = load_protocol(slot["config"]["protocol_name"])
    except Exception:
        pass

    # Auto-inject key parameters (use enactment_id as value if not supplied)
    for ref in safe_list(getattr(schema, "outs", [])):
        p       = ref.param if hasattr(ref, "param") else ref
        p_name  = str(getattr(p, "name", ref)).strip()
        is_key  = False
        if proto and hasattr(proto, "public_parameters"):
            param_obj = proto.public_parameters.get(p_name)
            is_key = (param_obj and getattr(param_obj, "key", None) == "key")
        if is_key and p_name not in req_payload and p_name not in bindings:
            req_payload[p_name] = enactment_id

    final_payload = {**bindings, **req_payload}

    # Privacy filter — only params the schema permits
    valid_params: set = set()
    for ref in safe_list(getattr(schema, "ins", [])) + safe_list(getattr(schema, "outs", [])):
        p = ref.param if hasattr(ref, "param") else ref
        valid_params.add(str(getattr(p, "name", p)).strip())
    filtered = {k: v for k, v in final_payload.items() if k in valid_params}

    # Build messages to send
    to_send: List[Message] = []
    recipients = safe_list(schema.recipients)
    if not recipients:
        to_send.append(Message(schema, filtered, meta={"enactment": enactment_id}, adapter=adapter, system="default"))
    else:
        for recip_role in recipients:
            r_name = str(getattr(recip_role, "name", recip_role)).strip()
            if r_name in topology:
                parsed = urlparse(topology[r_name])
                dest   = (parsed.hostname or "127.0.0.1", parsed.port or 80)
            else:
                dest = ("127.0.0.1", API_PORT)
            to_send.append(Message(schema, filtered, meta={"enactment": enactment_id}, dest=dest, adapter=adapter, system="default"))

    try:
        new_msgs   = [m for m in to_send if not adapter.history.context(m).find(m.schema)]
        retry_msgs = [m for m in to_send if     adapter.history.context(m).find(m.schema)]

        if new_msgs:
            await adapter.send(*new_msgs)
            record["bindings"].update(filtered)
            background_tasks.add_task(StateManager.save)

        for m in retry_msgs:
            await adapter.emitter.send(m)

        return {"status": "sent", "payload": filtered}
    except Exception as e:
        raise HTTPException(400, f"Send failed: {e}")


# ─────────────────────────────────────────────────────────────────
# Knowledge base — full history grouped by enactment key
# ─────────────────────────────────────────────────────────────────
@app.get("/{agent_id}/knowledge_base")
def get_knowledge_base(agent_id: str):
    if agent_id != AGENT_ID:
        raise HTTPException(404, "Unknown agent")

    result: Dict[str, dict] = {}
    for mas_id, slot in active_agents.items():
        adapter    = slot["adapter"]
        proto_name = slot["config"]["protocol_name"]
        try:
            proto, _ = load_protocol(proto_name)
        except Exception:
            continue

        # Collect key parameter names from the protocol
        key_params: set = set()
        if hasattr(proto, "public_parameters"):
            for p_name, param_obj in proto.public_parameters.items():
                if getattr(param_obj, "key", None) == "key":
                    key_params.add(p_name.strip())

        # Group history by enactment key value
        groups: Dict[str, list] = {}
        for m in adapter.history.messages():
            raw = {str(getattr(k, "name", k)).strip(): v for k, v in (getattr(m, "payload", {}) or {}).items()}
            key_val = next((raw[kp] for kp in key_params if kp in raw), "unkeyed")
            groups.setdefault(key_val, []).append({
                "message": getattr(getattr(m, "schema", None), "name", "?"),
                "payload": raw,
                "meta":    {k: str(v) for k, v in (getattr(m, "meta", {}) or {}).items()},
            })

        result[mas_id] = {
            "protocol":   proto_name,
            "role":       slot["config"]["role"],
            "enactments": groups,
            "key_params": list(key_params),
        }

    return {"agent_id": AGENT_ID, "knowledge_base": result}


# ─────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=API_PORT, reload=False)