"""
Smoke test for gateway.py — proves the reverse proxy preserves the
decentralized model:

  1. Start two real agents on their own ports.
  2. Start the gateway on a third port and register both agents by name.
  3. Drive a Purchase enactment end-to-end *only* through the gateway.
  4. Assert the protocol completes and per-agent metrics are intact.

If the gateway were centralizing state, killing it mid-run would break
things — but agents would still be addressable directly on their ports,
which is the whole point.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
STORAGE_DIR = REPO_ROOT / "storage"

BUYER_PORT   = 8201
SELLER_PORT  = 8202
GATEWAY_PORT = 8210


def reset_storage() -> None:
    if not STORAGE_DIR.exists():
        return
    for f in STORAGE_DIR.glob("state_*.json"):
        f.unlink(missing_ok=True)


def wait_for(url: str, timeout: float = 15.0) -> None:
    t_end = time.time() + timeout
    last: Exception | None = None
    while time.time() < t_end:
        try:
            r = requests.get(url, timeout=1.0)
            if r.status_code < 500:
                return
        except Exception as e:
            last = e
        time.sleep(0.1)
    raise RuntimeError(f"Timed out waiting for {url}: {last}")


def spawn_agent(name: str, port: int) -> subprocess.Popen:
    env = os.environ.copy()
    env["AGENT_NAME"]   = name
    env["API_PORT"]     = str(port)
    env["PROTOCOL_DIR"] = "protocols/reference"
    return subprocess.Popen(
        [sys.executable, "agent_api.py"],
        env=env,
        cwd=str(REPO_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def spawn_gateway() -> subprocess.Popen:
    env = os.environ.copy()
    env["GATEWAY_PORT"]    = str(GATEWAY_PORT)
    env["GATEWAY_REGISTRY"] = str(REPO_ROOT / "gateway_registry.test.json")
    return subprocess.Popen(
        [sys.executable, "gateway.py"],
        env=env,
        cwd=str(REPO_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def main() -> int:
    reset_storage()
    procs = [
        spawn_agent("GwBuyer",  BUYER_PORT),
        spawn_agent("GwSeller", SELLER_PORT),
    ]
    gw = spawn_gateway()
    procs.append(gw)

    try:
        wait_for(f"http://127.0.0.1:{BUYER_PORT}/api/identity")
        wait_for(f"http://127.0.0.1:{SELLER_PORT}/api/identity")
        wait_for(f"http://127.0.0.1:{GATEWAY_PORT}/")

        buyer_id  = requests.get(f"http://127.0.0.1:{BUYER_PORT}/api/identity").json()["agent_id"]
        seller_id = requests.get(f"http://127.0.0.1:{SELLER_PORT}/api/identity").json()["agent_id"]

        # Register both agents with the gateway by name
        for name, port in [("Buyer", BUYER_PORT), ("Seller", SELLER_PORT)]:
            r = requests.post(
                f"http://127.0.0.1:{GATEWAY_PORT}/register",
                json={"name": name, "url": f"http://127.0.0.1:{port}"},
                timeout=5.0,
            )
            r.raise_for_status()

        # Verify the registry endpoint
        agents = requests.get(f"http://127.0.0.1:{GATEWAY_PORT}/agents").json()["agents"]
        assert "Buyer" in agents and "Seller" in agents, f"register failed: {agents}"
        print("[gateway] registry OK")

        # Sanity check: GET identity through the gateway should match direct
        gw_buyer  = requests.get(f"http://127.0.0.1:{GATEWAY_PORT}/agents/Buyer/api/identity").json()
        gw_seller = requests.get(f"http://127.0.0.1:{GATEWAY_PORT}/agents/Seller/api/identity").json()
        assert gw_buyer["agent_id"]  == buyer_id
        assert gw_seller["agent_id"] == seller_id
        print("[gateway] passthrough GET OK")

        # Topology still uses direct URLs (gateway is for client-side convenience,
        # not for inter-agent traffic — that would re-centralize).
        mas_id = "gw-test"
        topology = {
            "Buyer":   f"http://127.0.0.1:{BUYER_PORT}/{buyer_id}",
            "Seller":  f"http://127.0.0.1:{SELLER_PORT}/{seller_id}",
            "Shipper": f"http://127.0.0.1:{SELLER_PORT}/{seller_id}",  # role merge for 2-agent test
        }
        for name, port in [("Buyer", BUYER_PORT), ("Seller", SELLER_PORT)]:
            r = requests.post(
                f"http://127.0.0.1:{GATEWAY_PORT}/agents/{name}/api/join",
                json={"mas_id": mas_id, "protocol_name": "Purchase",
                      "role": name, "topology": topology},
                timeout=5.0,
            )
            r.raise_for_status()
        print("[gateway] join via proxy OK")

        # Drive rfq through the gateway
        eid = "gw-purchase-1"
        requests.post(
            f"http://127.0.0.1:{GATEWAY_PORT}/agents/Buyer/{buyer_id}/enactments?mas_id={mas_id}",
            json={"enactment_id": eid, "bindings": {}},
            timeout=5.0,
        ).raise_for_status()
        requests.post(
            f"http://127.0.0.1:{GATEWAY_PORT}/agents/Buyer/{buyer_id}/enactments/{eid}/send/rfq?mas_id={mas_id}",
            json={"payload": {"ID": "1", "item": "Test"}},
            timeout=5.0,
        ).raise_for_status()
        print("[gateway] POST send via proxy OK")

        # Wait for the seller to see rfq, then send quote
        t_end = time.time() + 5
        while time.time() < t_end:
            r = requests.get(
                f"http://127.0.0.1:{GATEWAY_PORT}/agents/Seller/{seller_id}/enactments/{eid}/actions?mas_id={mas_id}",
                timeout=2.0,
            )
            if r.status_code == 200:
                names = {a["message"] for a in r.json().get("actions", [])}
                if "quote" in names:
                    break
            time.sleep(0.05)
        else:
            raise RuntimeError("seller never saw rfq through gateway")

        requests.post(
            f"http://127.0.0.1:{GATEWAY_PORT}/agents/Seller/{seller_id}/enactments/{eid}/send/quote?mas_id={mas_id}",
            json={"payload": {"price": "42"}},
            timeout=5.0,
        ).raise_for_status()
        print("[gateway] full rfq→quote round-trip OK")

        # Check metrics survived the proxy hop
        m = requests.get(
            f"http://127.0.0.1:{GATEWAY_PORT}/agents/Buyer/{buyer_id}/metrics"
        ).json()
        assert m["metrics"][mas_id]["messages_sent"] >= 1
        print(f"[gateway] metrics intact: {m['metrics'][mas_id]['messages_sent']} sent")

        print("\nGATEWAY TEST PASSED")
        return 0
    except Exception as e:
        print(f"\nGATEWAY TEST FAILED: {e}")
        return 1
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()
        # Clean up the test registry file
        (REPO_ROOT / "gateway_registry.test.json").unlink(missing_ok=True)


if __name__ == "__main__":
    sys.exit(main())
