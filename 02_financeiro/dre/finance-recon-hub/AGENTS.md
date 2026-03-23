# Clear Control Tower Agents

## Scope
Operating model for Clear Agro (BR/PY + USA scouting) using:
- Codex: execution engine (coding, scripts, tests, fixes)
- Synkra AIOS: orchestration layer (queues, routing, approvals, observability)

## Repetitive Jobs (8)
1. Daily bank reconciliation (Bling + bank statements).
2. Monthly financial close package (DRE, cash view, pending titles).
3. AR aging and collection priority list.
4. CRM lead hygiene and stage correction.
5. Seller weekly performance snapshot (pipeline, conversion, ticket).
6. Logistics exception monitoring (late deliveries, stock breaks).
7. Lab/P&D experiment registry and evidence pack.
8. USA market scouting digest (distributors, prices, regulation watch).

## Core Agents (5)
- `orchestrator`: intake, priority, SLA, routing, final sign-off gate.
- `finance-ops`: reconciliation, close, AP/AR analysis, risk flags.
- `revenue-ops`: CRM quality, seller cadence, funnel diagnostics.
- `ops-intel`: logistics, inventory, supplier and service incidents.
- `research-usa`: non-sensitive external research and synthesis.

## On-Demand Subagents
- `data-quality`: schema checks, null drift, duplicate detection.
- `compliance-lgpd`: sensitive-data scan and lawful-basis gate.
- `qa-automation`: test execution and regression checks.
- `incident-response`: runbook execution and rollback suggestion.

## Handoffs and Limits
- Every handoff includes: objective, input path, due time, acceptance criteria.
- No agent can self-approve production changes.
- `research-usa` cannot process personal/sensitive records.
- Any secrets exposure risk triggers `compliance-lgpd` review.

## Working Contract
- Track each run with `run_id`.
- Persist logs in `out/run_logs/*.jsonl`.
- Deliverables must meet `DEFINITION_OF_DONE.md`.
