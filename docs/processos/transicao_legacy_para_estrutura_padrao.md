# Transicao Legacy -> Estrutura Padrao

## Estrategia
Transicao sem downtime: arquivos criticos permanecem no caminho legacy, com espelhamento controlado para estrutura padrao.

## Mapeamento principal
- `00_governanca/roadmap_clear_os/*` -> `docs/roadmap/`
- `10_banco_de_dados/migracoes/*.sql` -> `archive/database_migrations_legacy/` (referencia historica)
- `supabase/migrations/*.sql` -> `supabase/migrations/` (trilha canonica executavel)
- `12_dashboards_interface/painel_financeiro/metabase/sql_cards/*.sql` -> `dashboards/metabase/`
- `13_logs_monitoramento/runbook_dashboard_financeiro_v1.md` -> `docs/runbooks/dashboard_financeiro_v1_runbook.md`
- `11_agentes_automacoes/12_integracoes_agent/pipeline/06_generate_bling_supabase_import.py` -> `integrations/bling/load/generate_bling_supabase_import.py`
- `11_agentes_automacoes/12_integracoes_agent/pipeline/09_reconcile_bling_supabase.py` -> `integrations/bling/reconciliation/reconcile_bling_supabase.py`
- `11_agentes_automacoes/12_integracoes_agent/pipeline/07_run_bling_supabase_daily.ps1` -> `integrations/bling/runners/run_bling_supabase_daily.ps1`
- `11_agentes_automacoes/12_integracoes_agent/pipeline/08_register_bling_supabase_daily_task.ps1` -> `integrations/bling/runners/register_bling_supabase_daily_task.ps1`

## Compatibilidade
- Scheduler diario (`CZ` e `CR`) ja migrado para wrappers em `automation/jobs` apontando para `integrations/bling/runners`.
- Estrutura legacy permanece ativa apenas como fallback durante estabilizacao.
- Migrations SQL executaveis devem existir somente em `supabase/migrations/`.

## Proxima fase recomendada
1. Congelar alteracoes fora da trilha canonica `supabase/migrations/`.
2. Monitorar duas semanas de execucao diaria sem regressao.
3. Apos estabilidade, arquivar referencias legacy remanescentes.
