#!/usr/bin/env python3
"""
Comprehensive end-to-end test for Sliq BSPL multi-agent system.
Tests: Purchase (3 agents), Logistics (4 agents), Auction (2 agents).

Usage (from project root):
    .venv/Scripts/python.exe test_comprehensive.py
"""

import json
import os
import subprocess
import sys
import time
from typing import Dict

import requests

BASE   = sys.executable
SCRIPT = "agent_api.py"

GREEN  = "\033[32m"
RED    = "\033[31m"
YELLOW = "\033[33m"
CYAN   = "\033[36m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

passes = fails = 0


def ok(msg):
    global passes
    passes += 1
    print(f"  {GREEN}✓{RESET} {msg}")


def fail(msg):
    global fails
    fails += 1
    print(f"  {RED}✗{RESET} {msg}")


def section(msg):
    print(f"\n{BOLD}{CYAN}── {msg} ──{RESET}")


def start_node(name: str, port: int) -> subprocess.Popen:
    env = os.environ.copy()
    env["AGENT_NAME"]    = name
    env["API_PORT"]      = str(port)
    env["PROTOCOL_DIR"]  = "protocols/reference"
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    return subprocess.Popen(
        [BASE, SCRIPT], env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        creationflags=flags,
    )


def wait_for(port: int, retries=24) -> bool:
    for _ in range(retries):
        try:
            if requests.get(f"http://127.0.0.1:{port}/api/identity", timeout=1).status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def uuid_of(port: int) -> str:
    return requests.get(f"http://127.0.0.1:{port}/api/identity").json()["agent_id"]


def join(port, mas_id, protocol, role, topology):
    return requests.post(
        f"http://127.0.0.1:{port}/api/join",
        json={"mas_id": mas_id, "protocol_name": protocol, "role": role, "topology": topology},
    ).json()


def start_eid(port, uuid, mas_id, eid):
    return requests.post(
        f"http://127.0.0.1:{port}/{uuid}/enactments?mas_id={mas_id}",
        json={"enactment_id": eid, "bindings": {}},
    ).json()


def actions(port, uuid, mas_id, eid):
    return requests.get(
        f"http://127.0.0.1:{port}/{uuid}/enactments/{eid}/actions?mas_id={mas_id}"
    ).json()


def send(port, uuid, mas_id, eid, msg, payload):
    return requests.post(
        f"http://127.0.0.1:{port}/{uuid}/enactments/{eid}/send/{msg}?mas_id={mas_id}",
        json={"payload": payload},
    ).json()


def wait_action(port, uuid, mas_id, eid, msg_name, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = actions(port, uuid, mas_id, eid)
        if msg_name in [a["message"] for a in data.get("actions", [])]:
            return data
        time.sleep(0.5)
    return actions(port, uuid, mas_id, eid)


def wait_invited(port, uuid, mas_id, eid, timeout=6) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        data = actions(port, uuid, mas_id, eid)
        if "detail" not in data:
            return True
        time.sleep(0.4)
    return False


# ═══════════════════════════════════════════════════════════════════
# TEST 1 — Purchase (Buyer + Seller + Shipper)
# ═══════════════════════════════════════════════════════════════════
def test_purchase(bp=8011, sp=8012, jp=8013):
    section("TEST 1: Purchase Protocol — 3 agents")
    procs = [start_node(n, p) for n, p in [("TBuyer","TestSeller" if False else "TestBuyer"), ("TestSeller","a"), ("TestShipper","b")]]
    # Restart properly
    for p in procs: p.terminate()
    procs = [start_node("TestBuyer", bp), start_node("TestSeller", sp), start_node("TestShipper", jp)]

    for port, role in [(bp,"Buyer"),(sp,"Seller"),(jp,"Shipper")]:
        if wait_for(port): ok(f"{role} node up (:{port})")
        else: fail(f"{role} node failed"); [p.terminate() for p in procs]; return

    b = uuid_of(bp); s = uuid_of(sp); j = uuid_of(jp)
    ok(f"UUIDs: Buyer={b[:8]}  Seller={s[:8]}  Shipper={j[:8]}")

    mas, eid = "p-mas", "p-e1"
    for port, role, top in [
        (bp, "Buyer",   {"Seller": f"http://127.0.0.1:{sp}/{s}", "Shipper": f"http://127.0.0.1:{jp}/{j}"}),
        (sp, "Seller",  {"Buyer":  f"http://127.0.0.1:{bp}/{b}", "Shipper": f"http://127.0.0.1:{jp}/{j}"}),
        (jp, "Shipper", {"Buyer":  f"http://127.0.0.1:{bp}/{b}", "Seller":  f"http://127.0.0.1:{sp}/{s}"}),
    ]:
        r = join(port, mas, "Purchase", role, top)
        (ok if r.get("status") == "joined" else fail)(f"{role} joined MAS")

    start_eid(bp, b, mas, eid)
    data = wait_action(bp, b, mas, eid, "rfq")
    (ok if "rfq" in [a["message"] for a in data.get("actions",[])] else fail)("Buyer: rfq enabled")

    r = send(bp, b, mas, eid, "rfq", {"item": "Laptop Pro", "ID": "42"})
    (ok if r.get("status")=="sent" else fail)("Buyer sent rfq")

    (ok if wait_invited(sp, s, mas, eid) else fail)("Seller auto-invited ✓")
    data = wait_action(sp, s, mas, eid, "quote")
    (ok if "quote" in [a["message"] for a in data.get("actions",[])] else fail)("Seller: quote enabled")
    r = send(sp, s, mas, eid, "quote", {"price": "999"})
    (ok if r.get("status")=="sent" else fail)("Seller sent quote")

    data = wait_action(bp, b, mas, eid, "accept")
    (ok if "accept" in [a["message"] for a in data.get("actions",[])] else fail)("Buyer: accept enabled")
    r = send(bp, b, mas, eid, "accept", {"resp": "yes", "address": "Lancaster, UK"})
    (ok if r.get("status")=="sent" else fail)("Buyer sent accept")

    data = wait_action(sp, s, mas, eid, "ship")
    (ok if "ship" in [a["message"] for a in data.get("actions",[])] else fail)("Seller: ship enabled")
    r = send(sp, s, mas, eid, "ship", {"shipped": "dispatched"})
    (ok if r.get("status")=="sent" else fail)("Seller sent ship")

    (ok if wait_invited(jp, j, mas, eid) else fail)("Shipper auto-invited ✓")
    data = wait_action(jp, j, mas, eid, "deliver")
    (ok if "deliver" in [a["message"] for a in data.get("actions",[])] else fail)("Shipper: deliver enabled")
    r = send(jp, j, mas, eid, "deliver", {"outcome": "delivered"})
    (ok if r.get("status")=="sent" else fail)("Shipper sent deliver — Purchase COMPLETE")

    kb = requests.get(f"http://127.0.0.1:{bp}/{b}/knowledge_base").json()
    msgs = [m["message"] for g in kb["knowledge_base"].get(mas,{}).get("enactments",{}).values() for m in g]
    ok(f"Buyer KB: {msgs}")
    [p.terminate() for p in procs]


# ═══════════════════════════════════════════════════════════════════
# TEST 2 — Logistics (Merchant, Labeler, Wrapper, Packer)
# ═══════════════════════════════════════════════════════════════════
def test_logistics(mp=8021, lp=8022, wp=8023, pp=8024):
    section("TEST 2: Logistics Protocol — 4 agents")
    nodes = [("TestMerchant",mp),("TestLabeler",lp),("TestWrapper",wp),("TestPacker",pp)]
    procs = [start_node(n, p) for n, p in nodes]
    uuids: Dict[str, str] = {}

    for name, port in nodes:
        role = name.replace("Test","")
        if wait_for(port):
            ok(f"{role} up (:{port})")
            uuids[role] = uuid_of(port)
        else:
            fail(f"{role} failed"); [p.terminate() for p in procs]; return

    ports = {r.replace("Test",""):p for r,p in nodes}
    def url(role): return f"http://127.0.0.1:{ports[role]}/{uuids[role]}"

    mas, eid = "log-mas", "log-e1"
    roles = ["Merchant","Labeler","Wrapper","Packer"]
    for role in roles:
        top = {r: url(r) for r in roles if r != role}
        r = join(ports[role], mas, "Logistics", role, top)
        (ok if r.get("status")=="joined" else fail)(f"{role} joined")

    start_eid(mp, uuids["Merchant"], mas, eid)

    # Merchant → Labeler
    data = wait_action(mp, uuids["Merchant"], mas, eid, "RequestLabel")
    (ok if "RequestLabel" in [a["message"] for a in data.get("actions",[])] else fail)("Merchant: RequestLabel")
    send(mp, uuids["Merchant"], mas, eid, "RequestLabel", {"orderID": "ord-99", "address": "London"})
    ok("Merchant sent RequestLabel")

    # Merchant → Wrapper
    data = wait_action(mp, uuids["Merchant"], mas, eid, "RequestWrapping")
    (ok if "RequestWrapping" in [a["message"] for a in data.get("actions",[])] else fail)("Merchant: RequestWrapping")
    send(mp, uuids["Merchant"], mas, eid, "RequestWrapping", {"itemID": "i-7", "item": "Crystal Vase"})
    ok("Merchant sent RequestWrapping")

    # Labeler
    (ok if wait_invited(lp, uuids["Labeler"], mas, eid) else fail)("Labeler auto-invited ✓")
    data = wait_action(lp, uuids["Labeler"], mas, eid, "Labeled")
    (ok if "Labeled" in [a["message"] for a in data.get("actions",[])] else fail)("Labeler: Labeled")
    r = send(lp, uuids["Labeler"], mas, eid, "Labeled", {"label": "FRAGILE-001"})
    (ok if r.get("status")=="sent" else fail)("Labeler sent Labeled")

    # Wrapper
    (ok if wait_invited(wp, uuids["Wrapper"], mas, eid) else fail)("Wrapper auto-invited ✓")
    data = wait_action(wp, uuids["Wrapper"], mas, eid, "Wrapped")
    (ok if "Wrapped" in [a["message"] for a in data.get("actions",[])] else fail)("Wrapper: Wrapped")
    r = send(wp, uuids["Wrapper"], mas, eid, "Wrapped", {"wrapping": "bubble-wrap"})
    (ok if r.get("status")=="sent" else fail)("Wrapper sent Wrapped")

    # Packer waits for both Labeled + Wrapped
    (ok if wait_invited(pp, uuids["Packer"], mas, eid) else fail)("Packer auto-invited ✓")
    data = wait_action(pp, uuids["Packer"], mas, eid, "Packed", timeout=12)
    (ok if "Packed" in [a["message"] for a in data.get("actions",[])] else fail)("Packer: Packed (after both deps)")
    r = send(pp, uuids["Packer"], mas, eid, "Packed", {"status": "ready"})
    (ok if r.get("status")=="sent" else fail)("Packer sent Packed — Logistics COMPLETE")

    [p.terminate() for p in procs]


# ═══════════════════════════════════════════════════════════════════
# TEST 3 — Auction (Auctioneer + Bidder)
# ═══════════════════════════════════════════════════════════════════
def test_auction(ap=8031, bp=8032):
    section("TEST 3: Auction Protocol — 2 agents (NEW)")
    procs = [start_node("TestAuctioneer", ap), start_node("TestBidder", bp)]

    for port, role in [(ap,"Auctioneer"),(bp,"Bidder")]:
        if wait_for(port): ok(f"{role} up (:{port})")
        else: fail(f"{role} failed"); [p.terminate() for p in procs]; return

    a = uuid_of(ap); b = uuid_of(bp)
    ok(f"Auctioneer={a[:8]}  Bidder={b[:8]}")

    mas, eid = "auct-mas", "auct-e1"
    r1 = join(ap, mas, "Auction", "Auctioneer", {"Bidder": f"http://127.0.0.1:{bp}/{b}"})
    r2 = join(bp, mas, "Auction", "Bidder", {"Auctioneer": f"http://127.0.0.1:{ap}/{a}"})
    (ok if r1.get("status")=="joined" else fail)("Auctioneer joined")
    (ok if r2.get("status")=="joined" else fail)("Bidder joined")

    start_eid(ap, a, mas, eid)
    data = wait_action(ap, a, mas, eid, "Open")
    (ok if "Open" in [x["message"] for x in data.get("actions",[])] else fail)("Auctioneer: Open enabled")

    r = send(ap, a, mas, eid, "Open", {"auctionID": "lot-777", "item": "Van Gogh"})
    (ok if r.get("status")=="sent" else fail)("Auctioneer sent Open")

    (ok if wait_invited(bp, b, mas, eid) else fail)("Bidder auto-invited ✓")
    data = wait_action(bp, b, mas, eid, "Bid")
    (ok if "Bid" in [x["message"] for x in data.get("actions",[])] else fail)("Bidder: Bid enabled after Open")

    r = send(bp, b, mas, eid, "Bid", {"bidID": "bid-001", "amount": "1500000"})
    (ok if r.get("status")=="sent" else fail)("Bidder sent Bid(1.5M)")

    data = wait_action(ap, a, mas, eid, "Award")
    (ok if "Award" in [x["message"] for x in data.get("actions",[])] else fail)("Auctioneer: Award enabled")

    r = send(ap, a, mas, eid, "Award", {"winner": "TestBidder"})
    (ok if r.get("status")=="sent" else fail)("Auctioneer sent Award — Auction COMPLETE")

    kb = requests.get(f"http://127.0.0.1:{ap}/{a}/knowledge_base").json()
    msgs = [m["message"] for g in kb["knowledge_base"].get(mas,{}).get("enactments",{}).values() for m in g]
    ok(f"Auctioneer KB: {msgs}")
    [p.terminate() for p in procs]


# ═══════════════════════════════════════════════════════════════════
# TEST 4 — API correctness
# ═══════════════════════════════════════════════════════════════════
def test_api(port=8041):
    section("TEST 4: API Correctness")
    proc = start_node("TestApi", port)
    if not wait_for(port): fail("Node start failed"); proc.terminate(); return
    ok(f"Node up (:{port})")

    u = uuid_of(port)
    r = requests.get(f"http://127.0.0.1:{port}/api/identity").json()
    assert "agent_id" in r and "webhook_base" in r
    ok(f"Identity OK: {r['agent_name']} / {r['agent_id'][:8]}…")

    protos = requests.get(f"http://127.0.0.1:{port}/protocols").json()["protocols"]
    assert "Purchase" in protos and "Logistics" in protos and "Auction" in protos, f"{protos}"
    ok(f"All 3 protocols listed: {protos}")

    spec = requests.get(f"http://127.0.0.1:{port}/protocols/Purchase").json()
    assert set(spec["roles"]) == {"Buyer","Seller","Shipper"}
    ok(f"Purchase spec: {spec['roles']}, {len(spec['messages'])} msgs")

    spec = requests.get(f"http://127.0.0.1:{port}/protocols/Auction").json()
    assert set(spec["roles"]) == {"Auctioneer","Bidder"}
    ok(f"Auction spec: {spec['roles']}, {[m['name'] for m in spec['messages']]}")

    r = requests.get(f"http://127.0.0.1:{port}/protocols/DoesNotExist")
    assert r.status_code == 404
    ok("404 on unknown protocol ✓")

    join(port, "dup", "Purchase", "Buyer", {})
    # Idempotent: rejoining the same MAS with the same role is a no-op (200 OK)
    r = requests.post(f"http://127.0.0.1:{port}/api/join",
                      json={"mas_id":"dup","protocol_name":"Purchase","role":"Buyer","topology":{}})
    assert r.status_code == 200 and r.json().get("status") == "joined", f"got {r.status_code}: {r.text}"
    ok("Idempotent rejoin returns 200 ✓")
    # But rejoining with a different role is a conflict (409)
    r = requests.post(f"http://127.0.0.1:{port}/api/join",
                      json={"mas_id":"dup","protocol_name":"Purchase","role":"Seller","topology":{}})
    assert r.status_code == 409, f"got {r.status_code}: {r.text}"
    ok("409 on role-conflict rejoin ✓")

    r = requests.post(f"http://127.0.0.1:{port}/bad-uuid/messages", json={"schema":"x"})
    assert r.status_code == 404
    ok("404 on unknown agent_id ✓")

    r = requests.get(f"http://127.0.0.1:{port}/{u}/dashboard")
    assert r.status_code == 200 and "Sliq" in r.text
    ok("Dashboard HTML served correctly ✓")

    proc.terminate()


# ═══════════════════════════════════════════════════════════════════
def reset_storage():
    """Wipe per-agent state files so tests start from a clean slate.
    The agent_registry.json is preserved so test agents keep stable UUIDs."""
    storage = "storage"
    if os.path.isdir(storage):
        for f in os.listdir(storage):
            if f.startswith("state_") and f.endswith(".json"):
                try:
                    os.remove(os.path.join(storage, f))
                except OSError:
                    pass
        # Also remove the legacy shared file if it exists
        legacy = os.path.join(storage, "system_state.json")
        if os.path.exists(legacy):
            try:
                os.remove(legacy)
            except OSError:
                pass


if __name__ == "__main__":
    print(f"\n{BOLD}Sliq Comprehensive Test Suite{RESET}")
    print("=" * 50)
    reset_storage()
    test_purchase()
    time.sleep(1)
    test_logistics()
    time.sleep(1)
    test_auction()
    time.sleep(1)
    test_api()
    section("RESULTS")
    total = passes + fails
    print(f"  {GREEN}{BOLD}{passes}/{total} passed{RESET}  {RED}{BOLD}{fails} failed{RESET}\n")
    sys.exit(0 if fails == 0 else 1)
