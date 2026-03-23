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

### Automacao do gate (publisher)
Executar:

`python scripts/finance_dashboard_publisher.py --config templates/default_config.yaml --run-id <RUN_ID>`

Saidas geradas em `out/dashboard_financeiro_v1/`:
- `dashboard_healthcheck.json`
- `finance_dashboard_publisher_<RUN_ID>_status.json`
- `launch_dashboard_financeiro_v1_<RUN_ID>.cmd`

Quando o gate falhar, o publisher gera alerta automatico em arquivo e tenta enviar Telegram se as variaveis estiverem configuradas:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

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

