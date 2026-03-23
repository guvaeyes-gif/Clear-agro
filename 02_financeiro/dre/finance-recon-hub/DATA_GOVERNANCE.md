# Data Governance

## Data Classes
- `Public`: external market/public info.
- `Internal`: operational metrics and process data.
- `Confidential`: finance, contracts, customer/vendor details.
- `Sensitive`: personal data (LGPD scope), credentials, tokens.

## Handling Rules
- Default classification: `Confidential` when unclear.
- Never commit secrets to repo.
- Use environment variables for tokens/keys.
- Mask personal identifiers in analysis exports when not required.
- Keep only minimum data needed for each task (data minimization).

## Access and Storage
- Input folders: `data/`
- Outputs: `out/`
- Logs: `out/run_logs/`
- Access by least privilege and role.

## Retention and Deletion
- Keep operational logs for audit period defined by business.
- Delete temporary extracts after validated consolidation.
- Record deletion actions in audit checklist.

## LGPD Guardrails
- Lawful basis must be explicit for personal data processing.
- Data subject requests must be logged and traceable.
- Incidents with personal data require immediate escalation.
