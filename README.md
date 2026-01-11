# BSPL Web Platform for Protocol-Based Multi-Agent Interaction

This repository contains the implementation of a web-based platform for enacting
multi-agent interaction protocols specified in **BSPL (Blindingly Simple Protocol Language)**.

BSPL enables correct and consistent communication in distributed systems by defining
interactions in terms of **information causality and protocol keys**, rather than rigid
message ordering.

The aim of this project is to demonstrate that protocol-driven interaction models can be
brought onto the web in a practical and developer-accessible manner, without requiring
users to understand BSPL syntax or multi-agent theory in depth.

This project is developed as a **Third-Year Project at Lancaster University**.

## Motivation

Distributed and multi-agent systems often suffer from communication errors due to
tight coupling, incorrect message sequencing, or inconsistent information flow.

BSPL addresses these issues by specifying *what information must be known* for an
interaction to occur, rather than prescribing *when* messages must be sent.

This project explores how BSPL protocols can be:
- compiled into web-accessible representations,
- enacted concurrently using key-based instance separation,
- exposed through a platform suitable for developers and non-expert users.

## What This Project Does

- Parses BSPL protocol specifications
- Compiles protocols into **role-specific interaction specifications**
- Supports multiple concurrent protocol enactments using protocol keys
- Records interaction events in an append-only event store
- Exposes protocol enactment functionality via a web API

The focus of the system is **communication correctness and interaction structure**,
not autonomous task execution.

## What This Project Does NOT Do

- It does not create autonomous agents that perform real-world actions
- It does not guarantee network reliability or message delivery
- It does not replace existing agent frameworks or AI agent toolkits

Instead, the platform provides **protocol-driven communication scaffolding**
that other systems can build upon.

## Dependencies and Libraries

BSPL is developed by Singh, Chopra, and Christie and is available at:

- https://github.com/masr/bspl
- https://github.com/shcv/bspl

This project uses a **forked version** of the BSPL repository:

- https://github.com/cagan-bayram/bspl

The fork extends the original BSPL implementation with an HTTP transport layer,
allowing BSPL-based interactions to be enacted over the web.

## Project Status

This repository is under active development.

Features are introduced incrementally and committed feature-by-feature to reflect
the design and implementation process of the system.

## License

This project is licensed under the MIT License.
