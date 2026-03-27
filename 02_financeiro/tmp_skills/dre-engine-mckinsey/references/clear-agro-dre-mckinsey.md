# Clear Agro DRE McKinsey

## Use In This Repository

Primary repo locations:
- `dre/finance-recon-hub/scripts/dre_engine_mckinsey_local.py`
- `dre/finance-recon-hub/scripts/adjust_dre_consistency.py`
- `dashboard_online/app.py`
- `dashboard_online/build_snapshot.py`

Use the skill when the request is about DRE generation or DRE logic. Use the dashboard files only when the request is about presentation or snapshot consumption.

## Supported Revenue Bases

- `cz_receita_liquida_ajustada`
  Use for the original CZ-only adjusted basis.
- `empresa_receita_operacional`
  Use for an operational company-wide basis without intercompany inflation.
- `empresa_receita_total_com_intercompany`
  Use only when the user explicitly wants total company revenue including intercompany effects.

If the request does not specify a basis, prefer `empresa_receita_operacional`.

## Core Formulas

The engine expects monthly revenue and cost frames joined by `mes_num`.

Derived metrics:
- `receita_liquida = selected_basis`
- `cmv_proxy = receita_liquida * cmv_pct`
- `lucro_bruto = receita_liquida - cmv_proxy`
- `margem_contribuicao = lucro_bruto - custos_variaveis_total`
- `ebitda = margem_contribuicao - custo_fixo_base`

## Expected Outputs

Typical outputs produced or patched by the engine:
- adjusted revenue CSV with `_consistente`
- `dre_scope_metadata.json`
- runtime config JSON for the delegated engine
- `dre_mckinsey_mensal.csv`
- status JSON containing `selected_revenue_scope_basis`

## Practical Checks

After a run, verify:
- the chosen basis exists in the revenue CSV
- `dre_mckinsey_mensal.csv` contains the expected monthly rows
- `cmv_proxy` is populated
- the status file points to the scope metadata file
- the narrative shown to users names the chosen basis explicitly

## Dashboard Integration Notes

When connecting the engine to the dashboard:
- keep business rules in the engine or snapshot layer
- keep labels and grouping in `dashboard_online/app.py`
- avoid duplicating formula logic in Streamlit if the snapshot can carry the derived value once
