# Operating Flows

## Flow: Feature CRM
1. Intake by `orchestrator` with objective and KPI impact.
2. `revenue-ops` details requirements and acceptance criteria.
3. Codex implements branch-level change and test/smoke checks.
4. `qa-automation` validates regression and data integrity.
5. `orchestrator` approves release and logs rollout decision.

## Flow: Monthly Financial Close
1. Trigger close window (`M+1 D1`) with close checklist.
2. `finance-ops` runs reconciliation and stale-data gate.
3. Generate outputs: close workbook, unmatched list, pendings.
4. Validate totals, variance thresholds, and exception causes.
5. Publish package and archive evidence with `run_id`.

## Flow: USA Research (No Sensitive Data)
1. `research-usa` receives scoped question set.
2. Pull public sources only (marketplaces, public reports, websites).
3. Prohibit customer/vendor PII and contract terms.
4. Generate short digest with confidence and source links.
5. `orchestrator` stores output as `Public/Internal` only.
