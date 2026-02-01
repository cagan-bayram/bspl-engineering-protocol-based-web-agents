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
# Config (ENV-driven)
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

# -------------------------
# Custom Dynamic Emitter
# -------------------------

class DynamicHTTPEmitter(HTTPEmitter):
    async def send(self, message) -> None:
        dest = message.dest
        
        dest_agent = None
        if isinstance(dest, tuple):
            dest_agent = self._endpoint_to_agent.get(dest)
        elif isinstance(dest, str):
            dest_agent = dest

        if dest_agent and dest_agent in self.base_urls:
            await super().send(message)
            return

        if isinstance(dest, tuple):
            host, port = dest
            url = f"http://{host}:{port}{self.path}"
            payload = self.encode(message)
            data = json.loads(payload)
            try:
                resp = await self.client.post(url, json=data)
                self.stats["bytes"] += len(payload)
                self.stats["packets"] += 1
                resp.raise_for_status()
            except Exception as exc:
                logger.error(f"Dynamic send failed to {url}: {exc}")
                raise
        else:
            raise RuntimeError(f"No base URL configured for destination agent '{dest}'")

# -------------------------
# State Management
# -------------------------

adapters: Dict[Tuple[str, str], Adapter] = {}
adapter_started: Dict[Tuple[str, str], bool] = {}
_adapter_init_lock = asyncio.Lock()

enactments: Dict[str, Dict[str, Any]] = {}

class StateManager:
    @staticmethod
    def save():
        data = {
            "enactments": enactments,
            "history": []
        }
        
        for adapter in adapters.values():
            for msg in adapter.history.messages():
                if not hasattr(msg, 'schema') or not hasattr(msg.schema, 'protocol'):
                    continue

                data["history"].append({
                    "protocol": msg.schema.protocol.name,
                    "message": msg.schema.name,
                    "payload": msg.payload,
                    "meta": msg.meta,
                    "system": msg.system,
                    "sender": msg.schema.sender.name if msg.schema.sender else None
                })
        
        try:
            with open(DATA_FILE, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            logger.info(f"State saved to {DATA_FILE}")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    @staticmethod
    async def load():
        if not DATA_FILE.exists():
            return

        try:
            with open(DATA_FILE, 'r') as f:
                data = json.load(f)
            
            enactments.update(data.get("enactments", {}))
            
            needed_adapters = set()
            for rec in enactments.values():
                needed_adapters.add((rec['protocol'], rec['role']))
            
            history_items = data.get("history", [])
            for item in history_items:
                needed_adapters.add((normalize_protocol_name(item['protocol']), AGENT_NAME))

            for prot, rol in needed_adapters:
                await ensure_adapter(prot, rol)

            for item in history_items:
                p_name = item['protocol']
                m_name = item['message']
                
                try:
                    _, spec = load_protocol(p_name)
                    proto = spec.protocols[normalize_protocol_name(p_name)]
                    if m_name not in proto.messages: continue
                    schema = proto.messages[m_name]
                    
                    msg = Message(
                        schema=schema,
                        payload=item['payload'],
                        meta=item.get('meta', {}),
                        system=item.get('system', 'default')
                    )
                    
                    for (ap, ar), adapter in adapters.items():
                        if ap == normalize_protocol_name(p_name):
                            # Allow adding duplicate messages to history during load to restore state fully
                            context = adapter.history.context(msg)
                            match = context.find(schema)
                            if not match:
                                adapter.history.add(msg)
                except Exception as ex:
                    logger.warning(f"Skipping history item {m_name}: {ex}")
                            
            logger.info(f"State loaded from {DATA_FILE}")
            
        except Exception as e:
            logger.error(f"Failed to load state: {e}")

# -------------------------
# FastAPI App
# -------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    await StateManager.load()
    yield
    StateManager.save()

app = FastAPI(title=f"Slik Agent: {AGENT_NAME}", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

app.mount("/static", StaticFiles(directory="."), name="static")

@app.get("/dashboard.html")
async def read_dashboard():
    return FileResponse('dashboard.html')

# -------------------------
# Models & Helpers
# -------------------------

class StartEnactmentBody(BaseModel):
    bindings: Dict[str, Any] = {}
    peers: Dict[str, str] = {} 

class SendBody(BaseModel):
    payload: Dict[str, Any]

def safe_list(x):
    try: return list(x)
    except: return []

def normalize_protocol_name(n: str) -> str:
    return (n[0].upper() + n[1:]) if n else n

def load_protocol(protocol_name: str):
    protocol_name = normalize_protocol_name(protocol_name)
    path = PROTOCOL_DIR / f"{protocol_name}.bspl"
    if not path.exists():
        raise HTTPException(404, f"Protocol {protocol_name} not found")
    spec = bspl.load_file(str(path))
    return spec.protocols[protocol_name], spec

def export_module(protocol_name: str, spec):
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    if str(GENERATED_DIR) not in sys.path: sys.path.insert(0, str(GENERATED_DIR))
    return spec.export(normalize_protocol_name(protocol_name)).module

async def ensure_adapter(protocol_name: str, role_name: str) -> Adapter:
    protocol_name = normalize_protocol_name(protocol_name)
    key = (protocol_name, role_name)
    
    if key in adapters: return adapters[key]

    async with _adapter_init_lock:
        if key in adapters: return adapters[key]

        proto, spec = load_protocol(protocol_name)
        mod = export_module(protocol_name, spec)

        emitter = DynamicHTTPEmitter(base_urls=BASE_URLS, path=MESSAGES_PATH)
        receiver = HTTPReceiver(host="0.0.0.0", port=RECEIVER_PORT, path=MESSAGES_PATH)

        roles_map = {}
        for r in proto.roles.values(): roles_map[r] = r.name
        for r in proto.roles.values():
            if hasattr(mod, r.name): roles_map[getattr(mod, r.name)] = r.name

        agents = {}
        for r in proto.roles.values():
            if r.name in BASE_URLS:
                parsed = urlparse(BASE_URLS[r.name])
                agents[r.name] = [(parsed.hostname or "127.0.0.1", parsed.port or 80)]
            else:
                agents[r.name] = [("127.0.0.1", RECEIVER_PORT)]

        adapter = Adapter(
            name=role_name,
            systems={"default": {"protocol": proto, "roles": roles_map}},
            agents=agents,
            emitter=emitter,
            receiver=receiver,
        )
        adapters[key] = adapter

        if not adapter_started.get(key):
            if not hasattr(adapter, "events") or adapter.events is None:
                from asyncio.queues import Queue
                adapter.events = Queue()
            
            adapter.running = True
            asyncio.create_task(adapter.update_loop())
            asyncio.create_task(receiver.task(adapter))
            await adapter.signal(InitEvent())
            adapter_started[key] = True

        return adapter

# -------------------------
# Endpoints
# -------------------------

@app.get("/vcard")
def get_vcard():
    protocols = []
    if PROTOCOL_DIR.exists():
        protocols = [p.stem for p in PROTOCOL_DIR.glob("*.bspl")]
    return {
        "agent": AGENT_NAME,
        "endpoint": f"http://{PUBLIC_HOST}:{RECEIVER_PORT}{MESSAGES_PATH}",
        "dashboard_api": f"http://{PUBLIC_HOST}:{API_PORT}",
        "supported_protocols": protocols
    }

@app.get("/protocols")
def list_protocols():
    if not PROTOCOL_DIR.exists(): return []
    return [p.stem for p in PROTOCOL_DIR.glob("*.bspl")]

@app.get("/protocols/{protocol_name}")
def get_protocol_details(protocol_name: str):
    proto, _ = load_protocol(protocol_name)
    return {
        "protocol": proto.name,
        "roles": list(proto.roles.keys()),
        "messages": list(proto.messages.keys()),
        "parameters": list(proto.keys)
    }

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

@app.get("/protocols/{protocol_name}/enactments/{enactment_id}/actions")
async def get_actions(protocol_name: str, enactment_id: str, role: Optional[str] = None):
    protocol_name = normalize_protocol_name(protocol_name)
    if not role: role = enactments.get(enactment_id, {}).get("role", AGENT_NAME)
    
    if enactment_id not in enactments: raise HTTPException(404, "Enactment not found")
    
    adapter = await ensure_adapter(protocol_name, role)
    record = enactments[enactment_id]
    bindings = record["bindings"]
    
    updated = False
    for m in list(adapter.enabled_messages.messages()):
        payload = getattr(m, "payload", {}) or {}
        if not payload: continue
        
        meta = getattr(m, "meta", {}) or {}
        is_mine = (meta.get("enactment") == enactment_id)
        if not is_mine and bindings:
            if all(k in payload and payload[k] == v for k, v in bindings.items()):
                is_mine = True
        
        if is_mine:
            for k, v in payload.items():
                if k not in bindings or bindings[k] != v:
                    bindings[k] = v
                    updated = True
    
    if updated: 
        StateManager.save()

    actions = []
    for m in adapter.enabled_messages.messages():
        schema = getattr(m, "schema", m)
        payload = getattr(m, "payload", {})
        meta = getattr(m, "meta", {})
        
        if meta.get("enactment") and meta.get("enactment") != enactment_id:
            continue
            
        actions.append({
            "message": schema.name,
            "ins": safe_list(getattr(schema, "ins", [])),
            "outs": safe_list(getattr(schema, "outs", [])),
            "known": payload
        })

    return {
        "enactment_id": enactment_id,
        "bindings": bindings,
        "peers": record.get("peers", {}),
        "actions": actions
    }

@app.post("/protocols/{protocol_name}/enactments/{enactment_id}/send/{message_name}")
async def send_message(
    protocol_name: str,
    enactment_id: str,
    message_name: str,
    body: SendBody,
    background_tasks: BackgroundTasks,
    role: Optional[str] = None,
):
    protocol_name = normalize_protocol_name(protocol_name)
    if not role: role = enactments.get(enactment_id, {}).get("role", AGENT_NAME)

    record = enactments[enactment_id]
    bindings = record["bindings"]
    peers = record.get("peers", {})

    adapter = await ensure_adapter(protocol_name, role)
    proto = adapter.systems["default"]["protocol"]
    if message_name not in proto.messages: raise HTTPException(404, "Message not found")
    
    schema = proto.messages[message_name]
    final_payload = {**bindings, **body.payload}

    to_send = []
    recipients = safe_list(schema.recipients)
    
    if not recipients:
        msg = Message(
            schema, 
            final_payload, 
            meta={"enactment": enactment_id}, 
            adapter=adapter,
            system="default" 
        )
        to_send.append(msg)
    else:
        for recip_role in recipients:
            dest_url = None
            role_name = getattr(recip_role, 'name', str(recip_role))
            
            if role_name in peers:
                try:
                    p = urlparse(peers[role_name])
                    if p.hostname and p.port:
                        dest_url = (p.hostname, p.port)
                        logger.info(f"Routing {role_name} to {dest_url} (Dynamic)")
                except:
                    logger.warning(f"Invalid peer URL for {role_name}: {peers[role_name]}")
            
            msg = Message(
                schema=schema, 
                payload=final_payload, 
                meta={"enactment": enactment_id}, 
                dest=dest_url,
                adapter=adapter,
                system="default" 
            )
            to_send.append(msg)

    # --- IMPROVED RETRY LOGIC ---
    try:
        new_messages = []
        retry_messages = []

        for m in to_send:
            # Check existence loosely (by schema and key parameters) rather than strict equality
            # This handles cases where metadata (like timestamps) differs between memory and fresh object
            context = adapter.history.context(m)
            match = context.find(m.schema)
            
            if match:
                # It exists in history -> FORCE RETRY
                retry_messages.append(m)
            else:
                # It is new -> Send via adapter (which adds to history)
                new_messages.append(m)

        if new_messages:
            await adapter.send(*new_messages)
            record["bindings"].update(final_payload)
            background_tasks.add_task(StateManager.save)

        if retry_messages:
            logger.info(f"Retrying transmission for {len(retry_messages)} messages.")
            for m in retry_messages:
                # Manually emit to bypass duplicate checks
                await adapter.emitter.send(m)

        return {"status": "sent", "payload": final_payload}

    except Exception as e:
        logger.error(f"Send failed: {e}")
        import traceback; traceback.print_exc()
        raise HTTPException(500, f"Send failed: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=API_PORT, reload=False)