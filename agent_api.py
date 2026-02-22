from __future__ import annotations

import asyncio
import os
import sys
import uuid
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional
from urllib.parse import urlparse
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

import bspl
from bspl.adapter.core import Adapter
from bspl.adapter.http_adapter import HTTPEmitter, HTTPReceiver
from bspl.adapter.event import InitEvent
from bspl.adapter.message import Message

# -------------------------
# Config (Static Topology)
# -------------------------
PROTOCOL_DIR = Path(os.getenv("PROTOCOL_DIR", "protocols/reference"))
GENERATED_DIR = Path(os.getenv("BSPL_GENERATED_DIR", ".bspl_generated")).resolve()

API_PORT = int(os.getenv("API_PORT", "8001"))
RECEIVER_PORT = int(os.getenv("RECEIVER_PORT", "9001"))
MESSAGES_PATH = os.getenv("MESSAGES_PATH", "/messages")
AGENT_NAME = os.getenv("AGENT_NAME", "GenericAgent").strip()
PUBLIC_HOST = os.getenv("PUBLIC_HOST", "127.0.0.1") 
DATA_FILE = Path(os.getenv("BSPL_DATA_FILE", f".agent_state_{AGENT_NAME}.json"))
BASE_URLS_RAW = os.getenv("BASE_URLS", "")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(f"agent_api.{AGENT_NAME}")

def parse_base_urls(raw: str) -> Dict[str, str]:
    raw = (raw or "").strip()
    if not raw: return {}
    out = {}
    for part in raw.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip()] = v.strip().rstrip("/")
    return out

BASE_URLS = parse_base_urls(BASE_URLS_RAW)

class DynamicHTTPEmitter(HTTPEmitter):
    async def send(self, message) -> None:
        dest = message.dest
        if isinstance(dest, tuple):
            host, port = dest
            url = f"http://{host}:{port}{self.path}"
            payload = self.encode(message)
            try:
                resp = await self.client.post(url, json=json.loads(payload))
                self.stats["bytes"] += len(payload)
                self.stats["packets"] += 1
                resp.raise_for_status()
            except Exception as exc:
                logger.error(f"Dynamic send failed to {url}: {exc}")
                raise
        else:
            raise RuntimeError(f"Unknown destination format: '{dest}'")

adapters: Dict[Tuple[str, str], Adapter] = {}
adapter_started: Dict[Tuple[str, str], bool] = {}
_adapter_init_lock = asyncio.Lock()
enactments: Dict[str, Dict[str, Any]] = {}

class StateManager:
    @staticmethod
    def save():
        data = {"enactments": enactments, "history": []}
        for adapter in adapters.values():
            for msg in adapter.history.messages():
                if hasattr(msg, 'schema') and hasattr(msg.schema, 'protocol'):
                    data["history"].append({
                        "protocol": msg.schema.protocol.name, "message": msg.schema.name,
                        "payload": msg.payload, "meta": msg.meta, "system": msg.system
                    })
        try:
            with open(DATA_FILE, 'w') as f: json.dump(data, f, indent=2, default=str)
        except Exception as e: logger.error(f"Failed to save state: {e}")

    @staticmethod
    async def load():
        if not DATA_FILE.exists(): return
        try:
            with open(DATA_FILE, 'r') as f: data = json.load(f)
            enactments.update(data.get("enactments", {}))
            
            needed_adapters = set((rec['protocol'], rec['role']) for rec in enactments.values())
            for prot, rol in needed_adapters: await ensure_adapter(prot, rol)

            for item in data.get("history", []):
                try:
                    _, spec = load_protocol(item['protocol'])
                    schema = spec.protocols[normalize_protocol_name(item['protocol'])].messages[item['message']]
                    msg = Message(schema=schema, payload=item['payload'], meta=item.get('meta', {}), system=item.get('system', 'default'))
                    for (ap, ar), adapter in adapters.items():
                        if ap == normalize_protocol_name(item['protocol']):
                            if not adapter.history.context(msg).find(schema): adapter.history.add(msg)
                except Exception: pass
        except Exception as e: logger.error(f"Failed to load state: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await StateManager.load()
    yield
    StateManager.save()

app = FastAPI(title=f"Sliq Agent: {AGENT_NAME}", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory="."), name="static")

@app.get("/dashboard.html")
async def read_dashboard(): return FileResponse('dashboard.html')

def safe_list(x):
    try: return list(x)
    except: return []

def normalize_protocol_name(n: str) -> str: return (n[0].upper() + n[1:]) if n else n

def load_protocol(protocol_name: str):
    protocol_name = normalize_protocol_name(protocol_name)
    path = PROTOCOL_DIR / f"{protocol_name.lower()}.bspl"
    if not path.exists(): path = PROTOCOL_DIR / f"{protocol_name}.bspl"
    if not path.exists(): raise HTTPException(404, f"Protocol {protocol_name} not found")
    spec = bspl.load_file(str(path))
    return spec.protocols[protocol_name], spec

async def ensure_adapter(protocol_name: str, role_name: str) -> Adapter:
    protocol_name = normalize_protocol_name(protocol_name)
    key = (protocol_name, role_name)
    if key in adapters: return adapters[key]

    async with _adapter_init_lock:
        if key in adapters: return adapters[key]
        proto, _ = load_protocol(protocol_name)
        emitter = DynamicHTTPEmitter(base_urls=BASE_URLS, path=MESSAGES_PATH)
        receiver = HTTPReceiver(host="0.0.0.0", port=RECEIVER_PORT, path=MESSAGES_PATH)

        roles_map = {r: r.name for r in proto.roles.values()}
        agents = {r.name: [(urlparse(BASE_URLS[r.name]).hostname or "127.0.0.1", urlparse(BASE_URLS[r.name]).port or 80)] if r.name in BASE_URLS else [("127.0.0.1", RECEIVER_PORT)] for r in proto.roles.values()}

        adapter = Adapter(name=role_name, systems={"default": {"protocol": proto, "roles": roles_map}}, agents=agents, emitter=emitter, receiver=receiver)
        adapters[key] = adapter

        if not adapter_started.get(key):
            from asyncio.queues import Queue
            adapter.events = Queue()
            adapter.running = True
            asyncio.create_task(adapter.update_loop())
            asyncio.create_task(receiver.task(adapter))
            await adapter.signal(InitEvent())
            adapter_started[key] = True

        return adapter

@app.get("/vcard")
def get_vcard(): return {"agent": AGENT_NAME, "endpoint": f"http://{PUBLIC_HOST}:{RECEIVER_PORT}{MESSAGES_PATH}"}

@app.get("/protocols")
def get_protocols():
    return {"protocols": ["Logistics", "Purchase"]}

@app.get("/protocols/{protocol_name}")
def get_protocol_details(protocol_name: str):
    proto, _ = load_protocol(protocol_name)
    return {"roles": list(proto.roles.keys())}

class StartEnactmentBody(BaseModel):
    bindings: Dict[str, Any] = {}
    peers: Dict[str, str] = {}

@app.post("/protocols/{protocol_name}/enactments")
async def start_enactment(
    protocol_name: str,
    body: StartEnactmentBody,
    background_tasks: BackgroundTasks,
    role: Optional[str] = Query(None),
    enactment_id: Optional[str] = Query(None),
):
    protocol_name = normalize_protocol_name(protocol_name)
    if not role and AGENT_NAME: role = AGENT_NAME
    if not role: raise HTTPException(400, "Role required")

    await ensure_adapter(protocol_name, role)

    eid = enactment_id.strip() if enactment_id else str(uuid.uuid4())
    
    enactments[eid] = {
        "protocol": protocol_name, 
        "role": role, 
        "bindings": dict(body.bindings or {}),
        "peers": dict(body.peers or {})
    }
    
    logger.info(f"Enactment {eid} started. Peers: {enactments[eid]['peers']}")
    background_tasks.add_task(StateManager.save)
    return {"protocol": protocol_name, "role": role, "enactment_id": eid, "bindings": enactments[eid]["bindings"]}

@app.get("/enactments/{enactment_id}/actions")
async def get_actions(enactment_id: str, role: Optional[str] = None):
    if enactment_id not in enactments: raise HTTPException(404, "Enactment not found")
    record = enactments[enactment_id]
    protocol_name = record["protocol"]
    role_name = str(role if role else record.get("role", AGENT_NAME))
    
    adapter = await ensure_adapter(protocol_name, role_name)
    bindings = record["bindings"]
    updated = False

    # 1. Update Knowledge Base strictly from actuality (History)
    if hasattr(adapter, "history"):
        for m in list(adapter.history.messages()):
            raw_payload = getattr(m, "payload", {}) or {}
            
            # CRITICAL: Force primitive string cast to annihilate AST Symbol bleed
            payload = {str(getattr(k, 'name', k)).strip(): v for k, v in raw_payload.items()}
            
            meta = getattr(m, "meta", {}) or {}
            is_mine = (meta.get("enactment") == enactment_id)
            
            # Universal match: Check meta tag OR see if the payload shares our Enactment ID
            if not is_mine and payload:
                if enactment_id in payload.values(): is_mine = True
                elif bindings:
                    shared_keys = set(payload.keys()).intersection(set(bindings.keys()))
                    if any(payload[k] == bindings[k] for k in shared_keys): is_mine = True
            
            if is_mine:
                for k, v in payload.items():
                    if k not in bindings or bindings[k] != v:
                        bindings[k] = v
                        updated = True
    if updated: StateManager.save()

    actions = []
    # 2. Process enabled actions
    for m in adapter.enabled_messages.messages():
        schema = getattr(m, "schema", m)
        
        # 3. Causality Cull: Remove schemas already actualized in history
        already_enacted = False
        if hasattr(adapter, "history"):
            for hist_m in adapter.history.messages():
                h_schema = getattr(hist_m, "schema", None)
                h_raw_payload = getattr(hist_m, "payload", {}) or {}
                
                # CRITICAL: Second string coercion block for the history cull
                h_payload = {str(getattr(k, 'name', k)).strip(): v for k, v in h_raw_payload.items()}
                h_meta = getattr(hist_m, "meta", {}) or {}
                
                h_is_mine = (h_meta.get("enactment") == enactment_id)
                if not h_is_mine and h_payload:
                    if enactment_id in h_payload.values(): h_is_mine = True
                    elif bindings:
                        s_keys = set(h_payload.keys()).intersection(set(bindings.keys()))
                        if any(h_payload[k] == bindings[k] for k in s_keys): h_is_mine = True

                if h_schema == schema and h_is_mine:
                    already_enacted = True
                    break
        if already_enacted: continue

        known_context = {getattr(p, "name", str(p)): bindings[getattr(p, "name", str(p))] 
                         for p in safe_list(getattr(schema, "ins", [])) 
                         if getattr(p, "name", str(p)) in bindings}
        
        outs_data = []
        for x in safe_list(getattr(schema, "outs", [])):
            p = x.param if hasattr(x, 'param') else x
            x_name = str(getattr(p, 'name', x))
            x_is_key = bool(getattr(p, 'key', False) or getattr(p, 'is_key', False) or 'key' in str(p).lower().split())
            if not x_is_key:
                outs_data.append(x_name)
            
        actions.append({
            "message": schema.name,
            "ins": [getattr(x, 'name', str(x)) for x in safe_list(getattr(schema, "ins", []))],
            "outs": outs_data,
            "known": known_context
        })

    return {"enactment_id": enactment_id, "bindings": bindings, "peers": record.get("peers", {}), "actions": actions}

class SendBody(BaseModel):
    payload: Dict[str, Any]

@app.post("/enactments/{enactment_id}/send/{message_name}")
async def send_message(enactment_id: str, message_name: str, body: dict, background_tasks: BackgroundTasks, role: Optional[str] = None):
    if enactment_id not in enactments: raise HTTPException(404, "Enactment not found")
    record = enactments[enactment_id]
    protocol_name = record["protocol"]
    role_name = str(role if role else record.get("role", AGENT_NAME))
    
    adapter = await ensure_adapter(protocol_name, role_name)
    bindings = record["bindings"]

    schema = next((getattr(m, "schema", m) for m in adapter.enabled_messages.messages() if getattr(m, "schema", m).name == message_name), None)
    if not schema: raise HTTPException(400, f"Message {message_name} not enabled or already sent")

    req_payload = body.get("payload", {})

    # 1. Inject Enactment ID for missing keys
    for ref in safe_list(getattr(schema, 'outs', [])):
        p = ref.param if hasattr(ref, 'param') else ref
        ref_name = str(getattr(p, 'name', ref))
        is_key = bool(getattr(p, 'key', False) or getattr(p, 'is_key', False) or 'key' in str(p).lower().split())
        
        if is_key and ref_name not in req_payload and ref_name not in bindings:
            req_payload[ref_name] = enactment_id

    final_payload = {**bindings, **req_payload}
    to_send = []
    recipients = safe_list(schema.recipients)
    
    # 2. Universal Deterministic Routing Matrix
    # 2. Universal Deterministic Routing Matrix (Pure Environment Variable Parsing)
    if not recipients:
        to_send.append(Message(schema, final_payload, meta={"enactment": enactment_id}, adapter=adapter, system="default"))
    else:
        for recip_role in recipients:
            r_name = str(getattr(recip_role, 'name', recip_role)).strip()
            
            # Pure dynamic parsing from the environment injected by the Launcher
            base_str = os.getenv("BASE_URLS", "")
            dynamic_urls = {}
            for pair in base_str.split(","):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    parsed = urlparse(v.strip())
                    dynamic_urls[k.strip()] = (parsed.hostname or "127.0.0.1", parsed.port or 80)
            
            if r_name in dynamic_urls:
                dest_url = dynamic_urls[r_name]
            else:
                # If strictly not found in environment topology, fallback to self (Receiver Port)
                dest_url = ("127.0.0.1", RECEIVER_PORT)
                
            to_send.append(Message(schema, final_payload, meta={"enactment": enactment_id}, dest=dest_url, adapter=adapter, system="default"))

    try:
        new_messages, retry_messages = [], []
        for m in to_send:
            if adapter.history.context(m).find(m.schema): retry_messages.append(m)
            else: new_messages.append(m)

        if new_messages:
            await adapter.send(*new_messages)
            record["bindings"].update(final_payload)
            background_tasks.add_task(StateManager.save)

        for m in retry_messages: await adapter.emitter.send(m)
        return {"status": "sent", "payload": final_payload}
    except Exception as e:
        raise HTTPException(400, f"System Error: {str(e)}")
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=API_PORT, reload=False)