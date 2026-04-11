"""
Sliq gateway — a transparent reverse proxy addressing the supervisor's
"ports should not be needed" feedback.

Each agent still runs as its own independent FastAPI process on its own
port. The gateway does not centralize any agent state — it only rewrites
URLs so that clients (dashboards, topologies, curl) can address agents by
*name* through a single entrypoint:

    http://localhost:8000/agents/Alice/<whatever>
      ↳ forwarded to
    http://127.0.0.1:8091/<whatever>

This preserves the decentralized architecture:
  * Each agent still runs in its own process and crashes independently.
  * The gateway holds *no* protocol state, no bindings, no history.
  * If the gateway dies, agents keep running — you just have to talk to
    them directly on their port again.
  * BSPL information causality is still enforced by the individual
    agents' adapters, not by the gateway.

What it gains for the report:
  * One stable URL to share with collaborators / UI.
  * Topologies become human-readable: `{"Seller": "http://localhost:8000/agents/Bob"}`.
  * Demonstrates that the "ports" complaint is a UX concern, not a
    fundamental limitation of the architecture.

Registry bootstrapping:
    1. The gateway reads `gateway_registry.json` on startup (if present).
    2. Agents can self-register at POST /register {"name": "...", "url": "..."}.
    3. Agents can be looked up at GET /agents.

Run:
    .venv/Scripts/python gateway.py
    # or:  GATEWAY_PORT=8000 .venv/Scripts/python gateway.py
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


GATEWAY_PORT  = int(os.getenv("GATEWAY_PORT", "8000"))
REGISTRY_FILE = Path(os.getenv("GATEWAY_REGISTRY", "gateway_registry.json"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s gateway: %(message)s")
logger = logging.getLogger("gateway")


# ─────────────────────────────────────────────────────────────────
# Registry
# ─────────────────────────────────────────────────────────────────
# Maps human-readable agent name -> base URL of the running agent.
# The base URL should NOT include a trailing slash and should NOT
# include an agent_id path segment — the client supplies that.
registry: Dict[str, str] = {}


def _load_registry() -> None:
    try:
        data = json.loads(REGISTRY_FILE.read_text())
    except FileNotFoundError:
        return
    except Exception as e:
        logger.warning(f"Could not read registry {REGISTRY_FILE}: {e}")
        return
    if isinstance(data, dict):
        registry.update({str(k): str(v).rstrip("/") for k, v in data.items()})
        logger.info(f"Loaded {len(registry)} agents from {REGISTRY_FILE}")


def _save_registry() -> None:
    try:
        REGISTRY_FILE.write_text(json.dumps(registry, indent=2))
    except Exception as e:
        logger.warning(f"Could not persist registry to {REGISTRY_FILE}: {e}")


# ─────────────────────────────────────────────────────────────────
# FastAPI app
# ─────────────────────────────────────────────────────────────────
# One shared async HTTP client — connection pooling matters for forwarding.
_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client
    _client = httpx.AsyncClient(timeout=30.0)
    _load_registry()
    yield
    if _client is not None:
        await _client.aclose()
        _client = None


app = FastAPI(title="Sliq Gateway", lifespan=lifespan)
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
        "name":    "Sliq Gateway",
        "purpose": "Transparent reverse proxy so clients can address agents by name.",
        "agents":  registry,
        "usage":   "http://localhost:{GATEWAY_PORT}/agents/{name}/<agent-api-path>",
    }


@app.get("/agents")
def list_agents():
    return {"agents": registry}


class RegisterBody(BaseModel):
    name: str
    url:  str


@app.post("/register")
def register(body: RegisterBody):
    name = body.name.strip()
    url  = body.url.strip().rstrip("/")
    if not name or not url:
        raise HTTPException(400, "Both 'name' and 'url' are required")
    registry[name] = url
    _save_registry()
    logger.info(f"Registered agent '{name}' -> {url}")
    return {"status": "registered", "name": name, "url": url}


@app.delete("/register/{name}")
def unregister(name: str):
    if name not in registry:
        raise HTTPException(404, f"Unknown agent '{name}'")
    del registry[name]
    _save_registry()
    return {"status": "unregistered", "name": name}


# ─────────────────────────────────────────────────────────────────
# Reverse proxy
# ─────────────────────────────────────────────────────────────────
# Hop-by-hop headers that must not be forwarded (RFC 7230 §6.1).
_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host",
    "content-length",  # recomputed by httpx based on body
}


async def _forward(name: str, subpath: str, request: Request) -> Response:
    if _client is None:
        raise HTTPException(503, "Gateway not ready")
    if name not in registry:
        raise HTTPException(404, f"Unknown agent '{name}'. Register it via POST /register.")

    base = registry[name]
    target = f"{base}/{subpath}" if subpath else base
    query = request.url.query
    if query:
        target = f"{target}?{query}"

    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in _HOP_BY_HOP
    }
    body = await request.body()

    try:
        upstream = await _client.request(
            method=request.method,
            url=target,
            headers=headers,
            content=body,
        )
    except httpx.RequestError as e:
        logger.warning(f"Forward to {target} failed: {e}")
        raise HTTPException(502, f"Agent '{name}' unreachable: {e}")

    resp_headers = {
        k: v for k, v in upstream.headers.items()
        if k.lower() not in _HOP_BY_HOP
    }
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=resp_headers,
        media_type=upstream.headers.get("content-type"),
    )


@app.api_route(
    "/agents/{name}/{subpath:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
async def proxy(name: str, subpath: str, request: Request):
    return await _forward(name, subpath, request)


@app.api_route(
    "/agents/{name}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
)
async def proxy_root(name: str, request: Request):
    return await _forward(name, "", request)


# ─────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting gateway on :{GATEWAY_PORT}")
    uvicorn.run(app, host="0.0.0.0", port=GATEWAY_PORT, reload=False)
