# Definition of Done

## Global Criteria
- Objective and business impact are explicit.
- Inputs, outputs, and owners are identified.
- Run executed with reproducible command.
- Logs stored under `out/run_logs/` with `run_id`.
- Validation checks passed (data + technical).
- Rollback path documented when change affects automation.

## Data Criteria
- Source freshness validated or execution marked `data_stale=true`.
- Required schema fields present.
- Null/duplicate checks executed for critical columns.
- Sensitive data treatment follows `DATA_GOVERNANCE.md`.

## Quality Criteria
- Script exits with non-zero on failure.
- Error message is actionable (root cause + next action).
- At least one smoke test executed post-change.

## Delivery Criteria
- Documentation updated in `README.md` or `docs/`.
- Checklist completed (`templates/checklists/`).
- Pending risks explicitly listed.
