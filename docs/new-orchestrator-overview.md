# COGNUS: Integrated Orchestrator Overview

## Status

- Baseline document: active
- Public branch snapshot date: March 11, 2026
- Source references: systematic mapping, primary paper, and low-level architecture

## 1. Purpose

COGNUS is an orchestrator for permissioned blockchain networks, currently centered on Hyperledger Fabric and designed around:

1. decentralized multi-organization governance;
2. auditable operational automation;
3. idempotent and resilient change execution;
4. observable operation with verifiable evidence.

The project goes beyond simply starting a network and installing chaincode. Its core idea is a control plane governed by changes, evidence, checkpoints, and guided operator flows. The public snapshot, however, exposes only the implemented baseline and not the entire long-term product vision.

## 2. Layered Architecture

### Layer 0: Infrastructure

- Hosts, VMs, Kubernetes or Docker, networking, and storage.
- Base security and isolation between environments.

### Layer 1: Execution network

- Fabric components such as peers, orderers, and certificate authorities.
- Channels, policies, ledger state, and domain chaincodes.

### Layer 2: Control plane

- Change governance, approval policies, and role boundaries.
- Evidence storage and artifact anchoring.
- Availability guardrails and preventive blocking logic.

### Layer 3: Orchestration and APIs

- Orchestration engine, gateway, hooks, and schedulers.
- Chaincode lifecycle and infrastructure automation.
- Supporting services such as views, interoperability, migration, and evaluation modules.

### Layer 4: Operations and UX

- Operator console, runbook catalog, dashboards, and audit views.
- Execution monitoring and platform health visibility.

## 3. Current Public Baseline

The current public snapshot includes the pieces that are implemented and demonstrable today:

- adapted Cello stack with API Engine and dashboard;
- SSH-based provisioning and lifecycle entry points;
- organization-aware flows for provisioning, runtime inspection, and inventory;
- post-commit hooks and artifact synchronization;
- gateway and marketplace synchronization scripts;
- operational dashboard for operator and auditor usage;
- public self-registration for `operator`.

## 4. Current Operational Flow

The implemented baseline already supports the following operator-oriented sequence:

1. register and authenticate in the dashboard;
2. open the provisioning workspace;
3. provide Linux host access data over SSH;
4. prepare and launch a guided provisioning run;
5. inspect checkpoints, evidence, inventory, and runtime state;
6. synchronize gateway and marketplace artifacts after lifecycle actions.

## 5. Public Snapshot Boundaries

This branch intentionally excludes:

- hardcoded admin bootstrap instructions;
- generated runtime state and cryptographic artifacts;
- mirrored upstream documentation;
- future-planning files and internal backlog material.

Future capabilities may still be discussed in the scientific material, but only the software and documentation required to understand, reproduce, and evaluate the current baseline are kept in `public-main`.

## 6. Related Documents

- Operations: `docs/auto-provisioning.md`, `docs/marketplace-workflow.md`
- Design system: `docs/visual-identity.md`, `docs/frontend/COGNUS_identidade_visual.md`
- Technical architecture: `docs/orchestrator-overview-macro.puml`, `docs/orchestrator-conceptual.puml`, `docs/orchestrator-architecture.puml`
