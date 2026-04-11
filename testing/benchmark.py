"""
Benchmark: decentralized (Sliq) vs centralized (centralized_api.py).

Runs N iterations of the Purchase protocol end-to-end on both systems and
writes a comparison table + CSV to `testing/benchmark_results.csv`.

This script is the quantitative half of the report's evaluation chapter.
The qualitative half lives in the report itself (single point of failure,
privacy enforcement, etc.).

Usage:
    .venv/Scripts/python testing/benchmark.py
    .venv/Scripts/python testing/benchmark.py --iterations 50
    .venv/Scripts/python testing/benchmark.py --iterations 20 --skip-decentralized
"""

from __future__ import annotations

import argparse
import csv
import os
import subprocess
import sys
import time
from pathlib import Path
from statistics import mean, median
from typing import Dict, List

import requests


REPO_ROOT = Path(__file__).resolve().parent.parent
STORAGE_DIR = REPO_ROOT / "storage"

DECENTRAL_BUYER_PORT   = 8101
DECENTRAL_SELLER_PORT  = 8102
DECENTRAL_SHIPPER_PORT = 8103
CENTRAL_PORT           = 9101


# ─────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────
def wait_for(url: str, timeout: float = 15.0) -> None:
    """Block until the given URL responds 2xx, or raise on timeout."""
    t_end = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < t_end:
        try:
            r = requests.get(url, timeout=1.0)
            if r.status_code < 500:
                return
        except Exception as e:
            last_err = e
        time.sleep(0.1)
    raise RuntimeError(f"Timed out waiting for {url}: {last_err}")


def reset_storage() -> None:
    """Wipe any stale per-agent state left by prior runs."""
    if not STORAGE_DIR.exists():
        return
    for f in STORAGE_DIR.glob("state_*.json"):
        f.unlink(missing_ok=True)
    (STORAGE_DIR / "system_state.json").unlink(missing_ok=True)


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * p
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return s[f]
    return s[f] + (s[c] - s[f]) * (k - f)


# ─────────────────────────────────────────────────────────────────
# Decentralized runner
# ─────────────────────────────────────────────────────────────────
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


def bench_decentralized(iterations: int) -> dict:
    print(f"\n[decentralized] starting 3 agents …")
    reset_storage()

    procs = [
        spawn_agent("BenchBuyer",   DECENTRAL_BUYER_PORT),
        spawn_agent("BenchSeller",  DECENTRAL_SELLER_PORT),
        spawn_agent("BenchShipper", DECENTRAL_SHIPPER_PORT),
    ]

    try:
        wait_for(f"http://127.0.0.1:{DECENTRAL_BUYER_PORT}/api/identity")
        wait_for(f"http://127.0.0.1:{DECENTRAL_SELLER_PORT}/api/identity")
        wait_for(f"http://127.0.0.1:{DECENTRAL_SHIPPER_PORT}/api/identity")

        buyer_id   = requests.get(f"http://127.0.0.1:{DECENTRAL_BUYER_PORT}/api/identity").json()["agent_id"]
        seller_id  = requests.get(f"http://127.0.0.1:{DECENTRAL_SELLER_PORT}/api/identity").json()["agent_id"]
        shipper_id = requests.get(f"http://127.0.0.1:{DECENTRAL_SHIPPER_PORT}/api/identity").json()["agent_id"]

        mas_id = "bench-mas"
        topology = {
            "Buyer":   f"http://127.0.0.1:{DECENTRAL_BUYER_PORT}/{buyer_id}",
            "Seller":  f"http://127.0.0.1:{DECENTRAL_SELLER_PORT}/{seller_id}",
            "Shipper": f"http://127.0.0.1:{DECENTRAL_SHIPPER_PORT}/{shipper_id}",
        }
        for port, role in [
            (DECENTRAL_BUYER_PORT,   "Buyer"),
            (DECENTRAL_SELLER_PORT,  "Seller"),
            (DECENTRAL_SHIPPER_PORT, "Shipper"),
        ]:
            r = requests.post(
                f"http://127.0.0.1:{port}/api/join",
                json={"mas_id": mas_id, "protocol_name": "Purchase", "role": role, "topology": topology},
                timeout=5.0,
            )
            r.raise_for_status()

        # Per-enactment wall-clock durations
        durations: List[float] = []
        print(f"[decentralized] running {iterations} Purchase enactments …")

        for i in range(iterations):
            eid = f"bench-{i}"
            t0 = time.time()

            # Create the enactment on the Buyer side
            requests.post(
                f"http://127.0.0.1:{DECENTRAL_BUYER_PORT}/{buyer_id}/enactments?mas_id={mas_id}",
                json={"enactment_id": eid, "bindings": {}},
                timeout=5.0,
            ).raise_for_status()

            # Buyer → Seller: rfq
            requests.post(
                f"http://127.0.0.1:{DECENTRAL_BUYER_PORT}/{buyer_id}/enactments/{eid}/send/rfq?mas_id={mas_id}",
                json={"payload": {"ID": str(i), "item": f"Item-{i}"}},
                timeout=5.0,
            ).raise_for_status()

            # Seller → Buyer: quote  (poll for the auto-created enactment on the Seller side)
            _wait_and_send(
                port=DECENTRAL_SELLER_PORT, agent_id=seller_id, mas_id=mas_id,
                eid=eid, msg="quote", payload={"price": "99"},
            )

            # Buyer → Seller: accept
            _wait_and_send(
                port=DECENTRAL_BUYER_PORT, agent_id=buyer_id, mas_id=mas_id,
                eid=eid, msg="accept", payload={"address": "221B Baker St", "resp": "ok"},
            )

            # Seller → Shipper: ship
            _wait_and_send(
                port=DECENTRAL_SELLER_PORT, agent_id=seller_id, mas_id=mas_id,
                eid=eid, msg="ship", payload={"shipped": "yes"},
            )

            # Shipper → Buyer: deliver
            _wait_and_send(
                port=DECENTRAL_SHIPPER_PORT, agent_id=shipper_id, mas_id=mas_id,
                eid=eid, msg="deliver", payload={"outcome": "delivered"},
            )

            # Wait until the Buyer actually has `outcome` bound (end of protocol)
            _wait_for_binding(
                port=DECENTRAL_BUYER_PORT, agent_id=buyer_id, mas_id=mas_id,
                eid=eid, param="outcome",
            )

            t1 = time.time()
            durations.append((t1 - t0) * 1000)

        # Pull final metrics from each agent
        buyer_metrics   = requests.get(f"http://127.0.0.1:{DECENTRAL_BUYER_PORT}/{buyer_id}/metrics").json()
        seller_metrics  = requests.get(f"http://127.0.0.1:{DECENTRAL_SELLER_PORT}/{seller_id}/metrics").json()
        shipper_metrics = requests.get(f"http://127.0.0.1:{DECENTRAL_SHIPPER_PORT}/{shipper_id}/metrics").json()

        return {
            "durations_ms":    durations,
            "buyer_metrics":   buyer_metrics,
            "seller_metrics":  seller_metrics,
            "shipper_metrics": shipper_metrics,
        }
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()


def _wait_and_send(port: int, agent_id: str, mas_id: str, eid: str, msg: str,
                   payload: Dict[str, str], timeout: float = 10.0) -> None:
    """Poll for the message to appear in the agent's enabled actions, then send."""
    t_end = time.time() + timeout
    while time.time() < t_end:
        r = requests.get(
            f"http://127.0.0.1:{port}/{agent_id}/enactments/{eid}/actions?mas_id={mas_id}",
            timeout=2.0,
        )
        if r.status_code == 200:
            actions = {a["message"] for a in r.json().get("actions", [])}
            if msg in actions:
                break
        time.sleep(0.02)
    else:
        raise RuntimeError(f"Timed out waiting for '{msg}' to become enabled on port {port}")

    requests.post(
        f"http://127.0.0.1:{port}/{agent_id}/enactments/{eid}/send/{msg}?mas_id={mas_id}",
        json={"payload": payload},
        timeout=5.0,
    ).raise_for_status()


def _wait_for_binding(port: int, agent_id: str, mas_id: str, eid: str,
                      param: str, timeout: float = 10.0) -> None:
    """Poll the actions endpoint (which reports bindings) until `param` is bound."""
    t_end = time.time() + timeout
    while time.time() < t_end:
        r = requests.get(
            f"http://127.0.0.1:{port}/{agent_id}/enactments/{eid}/actions?mas_id={mas_id}",
            timeout=2.0,
        )
        if r.status_code == 200 and param in (r.json().get("bindings") or {}):
            return
        time.sleep(0.02)
    raise RuntimeError(f"Timed out waiting for binding '{param}' at port {port}")


# ─────────────────────────────────────────────────────────────────
# Centralized runner
# ─────────────────────────────────────────────────────────────────
def spawn_central() -> subprocess.Popen:
    env = os.environ.copy()
    env["CENTRAL_PORT"]  = str(CENTRAL_PORT)
    env["PROTOCOL_DIR"]  = "protocols/reference"
    return subprocess.Popen(
        [sys.executable, "centralized_api.py"],
        env=env,
        cwd=str(REPO_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def bench_centralized(iterations: int) -> dict:
    print(f"\n[centralized] starting single server on :{CENTRAL_PORT} …")
    proc = spawn_central()
    base = f"http://127.0.0.1:{CENTRAL_PORT}"
    try:
        wait_for(f"{base}/")

        mas_id = "bench-mas"
        durations: List[float] = []
        print(f"[centralized] running {iterations} Purchase enactments …")

        for i in range(iterations):
            eid = f"bench-{i}"
            t0 = time.time()

            requests.post(
                f"{base}/enactments",
                json={"mas_id": mas_id, "protocol_name": "Purchase", "enactment_id": eid},
                timeout=5.0,
            ).raise_for_status()

            for msg, sender, payload in [
                ("rfq",     "Buyer",   {"ID": str(i), "item": f"Item-{i}"}),
                ("quote",   "Seller",  {"price": "99"}),
                ("accept",  "Buyer",   {"address": "221B Baker St", "resp": "ok"}),
                ("ship",    "Seller",  {"shipped": "yes"}),
                ("deliver", "Shipper", {"outcome": "delivered"}),
            ]:
                requests.post(
                    f"{base}/send",
                    json={
                        "mas_id":       mas_id,
                        "enactment_id": eid,
                        "sender_role":  sender,
                        "message_name": msg,
                        "payload":      payload,
                    },
                    timeout=5.0,
                ).raise_for_status()

            t1 = time.time()
            durations.append((t1 - t0) * 1000)

        metrics = requests.get(f"{base}/metrics").json()
        return {"durations_ms": durations, "metrics": metrics}
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()


# ─────────────────────────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────────────────────────
def summarize(label: str, durations: List[float]) -> dict:
    if not durations:
        return {"label": label, "n": 0}
    return {
        "label":  label,
        "n":      len(durations),
        "mean":   round(mean(durations), 3),
        "median": round(median(durations), 3),
        "min":    round(min(durations), 3),
        "max":    round(max(durations), 3),
        "p95":    round(percentile(durations, 0.95), 3),
        "p99":    round(percentile(durations, 0.99), 3),
    }


def print_table(rows: List[dict]) -> None:
    headers = ["system", "N", "mean_ms", "median_ms", "min_ms", "max_ms", "p95_ms", "p99_ms"]
    print()
    print("  ".join(f"{h:>11}" for h in headers))
    print("  ".join("-" * 11 for _ in headers))
    for r in rows:
        if r.get("n", 0) == 0:
            continue
        print("  ".join(f"{v:>11}" for v in [
            r["label"], r["n"], r["mean"], r["median"], r["min"], r["max"], r["p95"], r["p99"],
        ]))
    print()


def write_csv(path: Path, dec: dict, cen: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["iteration", "decentralized_ms", "centralized_ms"])
        dd = dec.get("durations_ms", [])
        cd = cen.get("durations_ms", [])
        for i in range(max(len(dd), len(cd))):
            w.writerow([
                i,
                f"{dd[i]:.3f}" if i < len(dd) else "",
                f"{cd[i]:.3f}" if i < len(cd) else "",
            ])


# ─────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--iterations", type=int, default=30)
    ap.add_argument("--skip-decentralized", action="store_true")
    ap.add_argument("--skip-centralized",   action="store_true")
    ap.add_argument("--csv", default="testing/benchmark_results.csv")
    args = ap.parse_args()

    dec_result: dict = {}
    cen_result: dict = {}

    if not args.skip_decentralized:
        dec_result = bench_decentralized(args.iterations)
    if not args.skip_centralized:
        cen_result = bench_centralized(args.iterations)

    rows = []
    if dec_result:
        rows.append(summarize("decentral", dec_result["durations_ms"]))
    if cen_result:
        rows.append(summarize("central",   cen_result["durations_ms"]))

    print("\n=== Purchase protocol wall-clock per enactment (ms) ===")
    print_table(rows)

    if dec_result:
        bm = dec_result["buyer_metrics"]["metrics"]
        mas_key = next(iter(bm.keys())) if bm else None
        if mas_key:
            b = bm[mas_key]
            print(f"[decentralized Buyer]  sent={b['messages_sent']}  received={b['messages_received']}  "
                  f"avg_send={b['avg_send_ms']}ms  max_send={b['max_send_ms']}ms  failures={b['send_failures']}")

    if cen_result:
        cm = cen_result["metrics"]["metrics"]
        mas_key = next(iter(cm.keys())) if cm else None
        if mas_key:
            c = cm[mas_key]
            print(f"[centralized server]   sent={c['messages_sent']}  received={c['messages_received']}  "
                  f"avg_send={c['avg_send_ms']}ms  max_send={c['max_send_ms']}ms  failures={c['send_failures']}")

    csv_path = REPO_ROOT / args.csv
    write_csv(csv_path, dec_result, cen_result)
    print(f"\nWrote CSV: {csv_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
