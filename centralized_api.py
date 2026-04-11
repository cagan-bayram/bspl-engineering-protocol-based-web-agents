"""
Centralized comparison baseline for the Sliq decentralized BSPL system.

This file is the *centralized* counterpart to `agent_api.py`. It exists for
the third-year-project report's evaluation chapter: it implements the same
BSPL protocols (Purchase, Logistics, Auction) but as a single process where
one server holds the state of every "agent" and routes every message
in-memory rather than over HTTP.

Key differences from the decentralized version, which the report should
highlight:

  * Single point of failure: if this process dies, every protocol enactment
    dies with it.
  * No privacy enforcement: every "agent" is just a key in the same dict.
    The Shipper *could* see the price, because there is no transport boundary
    that filters parameters.
  * No HTTP between agents: a "send" is a Python dict update, ~µs not ms.
  * One Adapter == one process. No process isolation.
  * No invitation model needed because the server sees everything.

To make the comparison fair, the centralized version still:
  * Loads the same .bspl protocol files
  * Enforces information causality (a message cannot be sent unless all `in`
    parameters are bound in the enactment's bindings dict)
  * Records the same metrics shape (messages_sent, messages_received,
    avg_send_ms, per-enactment durations)

Run:
    .venv/Scripts/python centralized_api.py
    # or:  CENTRAL_PORT=9000 .venv/Scripts/python centralized_api.py
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import bspl


# ─────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────
PROTOCOL_DIR = Path(os.getenv("PROTOCOL_DIR", "protocols/reference"))
CENTRAL_PORT = int(os.getenv("CENTRAL_PORT", "9000"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s central: %(message)s")
logger = logging.getLogger("central")


# ─────────────────────────────────────────────────────────────────
# Protocol loading (same .bspl files as the decentralized system)
# ─────────────────────────────────────────────────────────────────
def normalize_name(n: str) -> str:
    return (n[0].upper() + n[1:]) if n else n


@lru_cache(maxsize=None)
def load_protocol(protocol_name: str):
    # Cached: .bspl files don't change at runtime, and this is called from
    # every /send and /actions request. Without the cache, parsing dominates
    # the centralized server's measured latency in the benchmark.
    pn = normalize_name(protocol_name)
    path = PROTOCOL_DIR / f"{pn.lower()}.bspl"
    if not path.exists():
        path = PROTOCOL_DIR / f"{pn}.bspl"
    if not path.exists():
        raise HTTPException(404, f"Protocol '{pn}' not found in {PROTOCOL_DIR}")
    spec = bspl.load_file(str(path))
    return spec.protocols[pn]


def safe_list(x):
    try:
        return list(x)
    except Exception:
        return []


def msg_param_names(message_schema, attr: str) -> List[str]:
    """Extract parameter names from a BSPL message schema's `ins` or `outs`."""
    out = []
    for ref in safe_list(getattr(message_schema, attr, [])):
        p = ref.param if hasattr(ref, "param") else ref
        out.append(str(getattr(p, "name", p)).strip())
    return out


# ─────────────────────────────────────────────────────────────────
# Centralized state — every agent's state lives in this dict.
# ─────────────────────────────────────────────────────────────────
# enactments[mas_id][eid] = {
#     "protocol":  str,
#     "bindings":  Dict[str, str],     # all known parameter values
#     "history":   List[dict],         # append-only message log
#     "inbox":     Dict[role -> List[message_index]],
# }
enactments: Dict[str, Dict[str, dict]] = {}

# Per-mas metrics — same shape as decentralized for fair comparison
metrics: Dict[str, dict] = {}


def _ensure_mas(mas_id: str, protocol_name: str):
    if mas_id not in enactments:
        enactments[mas_id] = {}
    if mas_id not in metrics:
        metrics[mas_id] = {
            "protocol":          normalize_name(protocol_name),
            "joined_at":         time.time(),
            "messages_sent":     0,
            "messages_received": 0,
            "send_failures":     0,
            # Running aggregates instead of a per-send list — keeps memory
            # bounded under long benchmark runs and makes /metrics O(1) per MAS.
            "send_dur_sum_ms":   0.0,
            "send_dur_max_ms":   0.0,
            "enactments":        {},   # eid -> {first_ts, last_ts}
        }


# ─────────────────────────────────────────────────────────────────
# FastAPI app
# ─────────────────────────────────────────────────────────────────
app = FastAPI(title="Sliq Centralized Baseline")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {
        "name":          "Sliq Centralized Baseline",
        "purpose":       "Comparison baseline for the decentralized BSPL web platform",
        "active_mas":    list(enactments.keys()),
        "warning":       "All state is in one process. Restart wipes everything.",
    }


@app.get("/protocols")
def list_protocols():
    names: List[str] = []
    if PROTOCOL_DIR.exists():
        for f in sorted(PROTOCOL_DIR.glob("*.bspl")):
            names.append(normalize_name(f.stem))
    return {"protocols": names}


# ─────────────────────────────────────────────────────────────────
# Enactment lifecycle
# ─────────────────────────────────────────────────────────────────
class StartEnactBody(BaseModel):
    mas_id:        str
    protocol_name: str
    enactment_id:  Optional[str] = None


@app.post("/enactments")
def start_enactment(body: StartEnactBody):
    proto = load_protocol(body.protocol_name)
    _ensure_mas(body.mas_id, body.protocol_name)

    eid = (body.enactment_id or "").strip() or str(uuid.uuid4())
    if eid in enactments[body.mas_id]:
        return {"enactment_id": eid, "status": "already_exists"}

    # Initialize empty inbox per role
    inbox: Dict[str, List[int]] = {r.name: [] for r in proto.roles.values()}
    enactments[body.mas_id][eid] = {
        "protocol": normalize_name(body.protocol_name),
        "bindings": {},
        "history":  [],
        "inbox":    inbox,
    }
    return {"enactment_id": eid, "mas_id": body.mas_id, "status": "created"}


# ─────────────────────────────────────────────────────────────────
# Available actions for a role in an enactment
# ─────────────────────────────────────────────────────────────────
@app.get("/enactments/{eid}/actions")
def get_actions(eid: str, mas_id: str, role: str):
    if mas_id not in enactments or eid not in enactments[mas_id]:
        raise HTTPException(404, "Enactment not found")
    record = enactments[mas_id][eid]
    proto = load_protocol(record["protocol"])
    bindings = record["bindings"]

    actions = []
    sent_messages = {h["message"] for h in record["history"]}
    for msg in proto.messages.values():
        sender_name = str(getattr(msg.sender, "name", msg.sender))
        if sender_name != role:
            continue
        if msg.name in sent_messages:
            continue
        ins = msg_param_names(msg, "ins")
        if not all(p in bindings for p in ins):
            continue
        actions.append({
            "message":    msg.name,
            "sender":     sender_name,
            "recipients": [str(getattr(r, "name", r)) for r in safe_list(msg.recipients)],
            "ins":        ins,
            "outs":       msg_param_names(msg, "outs"),
        })

    return {"enactment_id": eid, "mas_id": mas_id, "role": role,
            "bindings": bindings, "actions": actions}


# ─────────────────────────────────────────────────────────────────
# Send a message — the central server validates causality and routes
# ─────────────────────────────────────────────────────────────────
class SendBody(BaseModel):
    mas_id:        str
    enactment_id:  str
    sender_role:   str
    message_name:  str
    payload:       Dict[str, Any] = {}


@app.post("/send")
def send_message(body: SendBody):
    t0 = time.time()
    if body.mas_id not in enactments or body.enactment_id not in enactments[body.mas_id]:
        raise HTTPException(404, "Enactment not found")
    record = enactments[body.mas_id][body.enactment_id]
    proto = load_protocol(record["protocol"])

    schema = proto.messages.get(body.message_name)
    if schema is None:
        raise HTTPException(404, f"Message '{body.message_name}' not in protocol")

    # Causality check — all `in` params must be bound already
    ins  = msg_param_names(schema, "ins")
    outs = msg_param_names(schema, "outs")
    bindings = record["bindings"]
    missing = [p for p in ins if p not in bindings]
    if missing:
        metrics[body.mas_id]["send_failures"] += 1
        raise HTTPException(400, f"Missing bound inputs: {missing}")

    # Sender role must match the protocol message
    sender_name = str(getattr(schema.sender, "name", schema.sender))
    if sender_name != body.sender_role:
        metrics[body.mas_id]["send_failures"] += 1
        raise HTTPException(400, f"Role '{body.sender_role}' cannot send '{body.message_name}' (sender is '{sender_name}')")

    # Compose final payload (the central server has access to ALL bindings —
    # no privacy filter is needed because there is no transport boundary).
    final_payload = {**{p: bindings[p] for p in ins if p in bindings}, **body.payload}

    # Update bindings with outs
    for p in outs:
        if p in body.payload:
            bindings[p] = body.payload[p]
        elif p in final_payload:
            bindings[p] = final_payload[p]

    # Append to history
    msg_index = len(record["history"])
    record["history"].append({
        "message":    body.message_name,
        "sender":     body.sender_role,
        "recipients": [str(getattr(r, "name", r)) for r in safe_list(schema.recipients)],
        "payload":    final_payload,
        "ts":         time.time(),
    })

    # Deliver: append the index to each recipient's inbox
    for r in safe_list(schema.recipients):
        rname = str(getattr(r, "name", r))
        record["inbox"].setdefault(rname, []).append(msg_index)

    t1 = time.time()
    duration_ms = round((t1 - t0) * 1000, 4)
    m = metrics[body.mas_id]
    m["messages_sent"] += 1
    m["messages_received"] += len(safe_list(schema.recipients))
    m["send_dur_sum_ms"] += duration_ms
    if duration_ms > m["send_dur_max_ms"]:
        m["send_dur_max_ms"] = duration_ms
    enact_m = m["enactments"].setdefault(body.enactment_id, {"first_ts": t1, "last_ts": t1})
    enact_m["first_ts"] = min(enact_m["first_ts"], t0)
    enact_m["last_ts"]  = max(enact_m["last_ts"],  t1)

    return {"status": "sent", "message": body.message_name, "payload": final_payload, "latency_ms": duration_ms}


# ─────────────────────────────────────────────────────────────────
# Inbox for a role (the centralized version's "receive")
# ─────────────────────────────────────────────────────────────────
@app.get("/inbox")
def get_inbox(mas_id: str, enactment_id: str, role: str):
    if mas_id not in enactments or enactment_id not in enactments[mas_id]:
        raise HTTPException(404, "Enactment not found")
    record = enactments[mas_id][enactment_id]
    indices = record["inbox"].get(role, [])
    return {
        "role":     role,
        "messages": [record["history"][i] for i in indices],
    }


# ─────────────────────────────────────────────────────────────────
# Full state — impossible in the decentralized version, trivial here.
# This is a key argument for the report: centralization gives global
# observability for free, but at the cost of privacy and resilience.
# ─────────────────────────────────────────────────────────────────
@app.get("/state")
def get_state():
    return {"enactments": enactments}


# ─────────────────────────────────────────────────────────────────
# Metrics — same shape as the decentralized agent's /metrics endpoint
# ─────────────────────────────────────────────────────────────────
@app.get("/metrics")
def get_metrics():
    out: Dict[str, dict] = {}
    for mas_id, m in metrics.items():
        sent = m["messages_sent"]
        per_enact = {
            eid: {
                **em,
                "duration_ms": round((em["last_ts"] - em["first_ts"]) * 1000, 3),
            }
            for eid, em in m["enactments"].items()
        }
        out[mas_id] = {
            "protocol":          m["protocol"],
            "joined_at":         m["joined_at"],
            "messages_sent":     sent,
            "messages_received": m["messages_received"],
            "send_failures":     m["send_failures"],
            "avg_send_ms":       round(m["send_dur_sum_ms"] / sent, 4) if sent else 0,
            "max_send_ms":       round(m["send_dur_max_ms"], 4),
            "enactments":        per_enact,
        }
    return {"metrics": out}


# ─────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting centralized baseline on :{CENTRAL_PORT}")
    uvicorn.run(app, host="0.0.0.0", port=CENTRAL_PORT, reload=False)
