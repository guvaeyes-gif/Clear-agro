# Integracao Bling

## Submodulos
- `extract/`: extracao do ERP
- `transform/`: normalizacao e padronizacao
- `load/`: geracao/aplicacao de carga no Supabase
- `reconciliation/`: reconcilizacao Bling x Supabase
- `runners/`: scripts de execucao diaria e registro de task
- `config/`: configs de pipeline

## Estado atual
Integracao ativa para empresas `CZ` e `CR` com automacao diaria.

## Fonte canonica de scripts (fase atual)
- Runner diario: `integrations/bling/runners/run_bling_supabase_daily.ps1`
- Registro scheduler: `integrations/bling/runners/register_bling_supabase_daily_task.ps1`
- Gerador de carga SQL: `integrations/bling/load/generate_bling_supabase_import.py`
- Reconciliacao: `integrations/bling/reconciliation/reconcile_bling_supabase.py`
- Configs de ingestao:
  - `integrations/bling/config/bling_ingest_hub_v1_cz.json`
  - `integrations/bling/config/bling_ingest_hub_v1_cr.json`

Estrutura legacy foi mantida apenas para compatibilidade transitoria.
