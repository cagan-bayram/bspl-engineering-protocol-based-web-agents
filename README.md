# Sliq — Protocol-Based Multi-Agent Web Platform

Sliq is a web platform for enacting multi-agent interaction protocols specified
in **BSPL (Blindingly Simple Protocol Language)**. Each agent runs as an
independent FastAPI web server and communicates with peers over HTTP. Protocol
correctness — including information causality and privacy — is enforced locally
by each agent using BSPL's formal model.

This project is developed as a **Third-Year Project at Lancaster University**.

## Motivation

Distributed and multi-agent systems often suffer from communication errors due
to tight coupling, incorrect message sequencing, or inconsistent information
flow. BSPL addresses these issues by specifying *what information must be known*
for an interaction to occur, rather than prescribing *when* messages must be
sent.

This project explores how BSPL protocols can be enacted on the web in a
decentralised, peer-to-peer manner — with no central coordinator — and compares
this architecture against a centralised baseline.

## What This Project Does

- Parses BSPL protocol specifications and enforces information causality locally
- Runs each agent as an independent web server with its own browser dashboard
- Supports multiple concurrent protocol enactments via protocol key separation
- Enforces privacy structurally — each agent only sees parameters its role is permitted to know
- Allows agents to crash and recover via message history replay
- Provides a centralised comparison system for benchmarking

## What This Project Does NOT Do

- It does not create autonomous agents that perform real-world actions
- It does not guarantee network reliability or message delivery
- It does not replace existing agent frameworks or AI agent toolkits

## Dependencies

BSPL is developed by Singh, Chopra, and Christie:

- https://github.com/masr/bspl
- https://github.com/shcv/bspl

This project uses a **forked version**:

- https://github.com/cagan-bayram/bspl

The fork extends the original BSPL implementation with an HTTP transport layer.

## Repository Structure

```
agent_api.py            # Core agent: FastAPI server, BSPL adapter, dashboard
centralized_api.py      # Centralised baseline for benchmarking
gateway.py              # Optional reverse proxy (single-port routing)
launcher_ui.py          # Tkinter GUI launcher for agent_api.py
Sliq.bat                # Windows launcher (opens the GUI)
dashboard.html          # Single-file browser dashboard (served by agent_api.py)
requirements.txt        # Python dependencies
protocols/
    reference/          # BSPL protocol specification files (9 protocols)
storage/
    agent_registry.json # Runtime agent name-to-port registry
testing/
    test_comprehensive.py     # End-to-end test suite (Purchase, Logistics)
    benchmark.py              # Decentralised vs centralised benchmark
    benchmark_results.csv     # Results from 10-iteration benchmark run
    test_gateway.py           # Gateway routing tests
    setup_ui_test.py          # Helper to launch agents for manual UI testing
```

## Running the Platform (Windows)

### Option 1 — GUI Launcher (recommended)

Double-click **`Sliq.bat`** to open the launcher. Set an agent name and port,
then click Launch. The agent starts and your browser opens its dashboard
automatically. Repeat for each agent in the protocol.

### Option 2 — Command Line

```bash
# Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Launch an agent (name and port are required)
set AGENT_NAME=Alice
set API_PORT=8001
python agent_api.py
```

## Running the Tests

```bash
python testing/test_comprehensive.py
```

Runs three test groups: Purchase (3 agents), Logistics (4 agents), and API
correctness checks.

## Running the Benchmark

```bash
python testing/benchmark.py
```

Runs 30 iterations of the Purchase protocol on both the decentralised and
centralised systems and outputs a comparison table.

## License

This project is licensed under the MIT License.
