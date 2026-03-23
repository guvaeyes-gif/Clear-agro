# Runbook - Dashboard Financeiro v1

## Objetivo
Garantir publicacao diaria confiavel do dashboard financeiro no Metabase.

## Janela operacional
- 06:10: job `CZ` (`ClearOS-Bling-Supabase-Daily-CZ`)
- 06:25: job `CR` (`ClearOS-Bling-Supabase-Daily-CR`)
- 06:40: refresh recomendado dos cards no Metabase

## Gate de qualidade (obrigatorio)
Antes de publicar:

1. Verificar `public.vw_finance_data_quality_banner`
2. Publicar somente se `quality_status = 'PASS'`
3. Se `FAIL`, mostrar banner "Dados em validacao"

## Evidencias minimas
Arquivos em `11_agentes_automacoes/12_integracoes_agent/pipeline/out/status`:

- `bling_supabase_reconciliation_*_status.json`
- `bling_supabase_reconciliation_*_qa.csv`
- `finance_ingest_hub_*_status.json`
- `bling_import_generator_*_status.json`

## Procedimento de falha
1. Identificar empresa afetada (`CZ` ou `CR`) no status de reconciliacao.
2. Reexecutar pipeline da empresa:
   - `07_run_bling_supabase_daily.ps1 -Company CZ`
   - ou `07_run_bling_supabase_daily.ps1 -Company CR`
3. Confirmar nova reconciliacao com `PASS`.
4. Atualizar dashboard no Metabase.

## Consultas de diagnostico
- `SELECT * FROM public.vw_finance_data_quality_banner;`
- `SELECT * FROM public.vw_finance_kpis_daily ORDER BY snapshot_date DESC, company;`
- `SELECT * FROM public.vw_finance_cash_projection_30d WHERE company='ALL' ORDER BY ref_date;`

