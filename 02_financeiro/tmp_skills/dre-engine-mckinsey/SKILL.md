---
name: dre-engine-mckinsey
description: Build, regenerate, audit, or adapt a McKinsey-style DRE for Clear Agro using the finance-recon-hub exports and the established revenue-scope rules. Use when Codex needs to compare revenue bases, align CMV proxy and fixed/variable costs, run the DRE engine from CSV exports, or explain/patch the McKinsey DRE workflow in this repository.
---

# DRE Engine Mckinsey

Use this skill to work on the Clear Agro McKinsey DRE flow.

## Follow This Workflow

1. Confirm the working context.
   Use this skill when the task touches `dre/finance-recon-hub`, McKinsey DRE exports, revenue-scope alignment, or EBITDA/CMV proxy logic.

2. Prefer the bundled engine.
   Run [scripts/dre_engine_mckinsey.py](scripts/dre_engine_mckinsey.py) when the user needs a repeatable DRE build from a config file.

3. Select the revenue basis explicitly.
   Supported bases are:
   - `cz_receita_liquida_ajustada`
   - `empresa_receita_operacional`
   - `empresa_receita_total_com_intercompany`

4. Keep scope consistent.
   When adjusting the DRE, align the revenue basis before recalculating `cmv_proxy`, `lucro_bruto`, `margem_contribuicao`, and `ebitda`.

5. Validate outputs after each run.
   Check that the run produced:
   - `dre_mckinsey_mensal.csv`
   - runtime config JSON
   - scope metadata JSON
   - status JSON with the selected basis

## Run The Engine

Prepare a config JSON or YAML with:
- `output_dir`
- `status_dir`
- `revenue.path`
- `revenue.mes_col`
- `revenue.receita_bruta_col`
- `revenue.devolucoes_col`
- `revenue.receita_liquida_col`
- `costs.path`
- `revenue_scope_basis`
- optional `cmv_pct`

Run:

```bash
python scripts/dre_engine_mckinsey.py --config <config-path> --run-id <run-id>
```

## Use Project References

Read [references/clear-agro-dre-mckinsey.md](references/clear-agro-dre-mckinsey.md) when you need:
- input and output expectations
- basis-selection guidance
- formula reminders
- repo-specific file locations

## Operating Rules

- Prefer the repository's existing exports and staging files over inventing new schemas.
- Keep naming aligned with the current project outputs.
- Do not change the revenue basis implicitly; record the chosen basis in outputs and explanations.
- If the task is a dashboard change, keep the DRE engine logic and the Streamlit presentation logic separated.
