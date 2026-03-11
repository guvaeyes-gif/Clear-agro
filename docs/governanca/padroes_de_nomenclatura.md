# Padroes de Nomenclatura

## Regras gerais
- usar minusculas
- evitar espacos
- preferir `snake_case`
- nomes autoexplicativos

## Scripts
- formato: `<acao>_<alvo>_<contexto>.py|.ps1|.cmd`
- exemplos:
  - `run_bling_supabase_daily.ps1`
  - `reconcile_bling_supabase.py`

## Migrations
- formato: `<yyyymmddhhmmss>_<descricao>.sql`
- exemplo: `20260311152000_finance_dashboard_v1_views.sql`

## Views SQL
- formato: `vw_<dominio>_<tema>`
- exemplos:
  - `vw_finance_kpis_daily`
  - `vw_finance_cash_projection_30d`

## Runners e jobs
- runner: `run_<pipeline>.ps1`
- scheduler: `register_<pipeline>_task.ps1`

