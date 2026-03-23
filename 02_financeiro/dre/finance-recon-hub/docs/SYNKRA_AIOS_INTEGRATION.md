# Synkra AIOS Integration Guide

## Suggested Project Structure
- `agents/` agent manifests and routing policy
- `workflows/` reusable orchestrated flows
- `connectors/` ERP/CRM/Sheets/Telegram adapters
- `policies/` approvals, risk, and compliance gates

## Command Patterns (example)
```bash
npx aios-core init clear-control-tower
npx aios-core workflow create monthly-fin-close
npx aios-core run monthly-fin-close --input ./data/bank/inbox
npx aios-core monitor --tail
```

## AIOS vs Codex Decision Rule
- Use `AIOS` when you need orchestration:
  - multi-step routing
  - queue/prioritization
  - approvals and observability
- Use `Codex multi-agent` when you need execution depth:
  - implement/refactor scripts
  - fix runtime failures
  - build tests and harden pipelines

## Minimal Integration Pattern
1. AIOS triggers a job with metadata (`job_type`, `run_id`, SLA).
2. Codex executes scripts in repo and returns artifacts + status.
3. AIOS persists status, routes exceptions, and notifies channels.
