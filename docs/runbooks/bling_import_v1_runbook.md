# Bling Import v1 - Runbook

## Objetivo
Importar dados do Bling para o Supabase em modo incremental e idempotente.

## Escopo v1
- `suppliers` (via contatos usados em contas a pagar)
- `customers` (via contatos de contas a receber)
- `accounts_payable`
- `accounts_receivable`

## Entradas
- `CRM_Clear_Agro/bling_api/contatos_cache.jsonl`
- `CRM_Clear_Agro/bling_api/contas_pagar_cache.jsonl`
- `CRM_Clear_Agro/bling_api/contas_receber_cache.jsonl`

## Artefatos do pipeline
- Config ingest-hub: `11_agentes_automacoes/12_integracoes_agent/pipeline/configs/bling_ingest_hub_v1.json`
- Gerador SQL: `11_agentes_automacoes/12_integracoes_agent/pipeline/06_generate_bling_supabase_import.py`
- Runner diario: `11_agentes_automacoes/12_integracoes_agent/pipeline/07_run_bling_supabase_daily.ps1`
- Registro de task diaria: `11_agentes_automacoes/12_integracoes_agent/pipeline/08_register_bling_supabase_daily_task.ps1`
- Migrations aplicadas:
  - `supabase/migrations/20260311101915_bling_import_v1.sql`
  - `supabase/migrations/20260311120000_bling_financial_transactions_v1.sql`

## Execucao (passo a passo)
1. Rodar staging/QA de ingestao:
```powershell
python C:/Users/cesar.zarovski/.codex/skills/finance-ingest-hub/scripts/finance_ingest_hub.py --config C:/Users/cesar.zarovski/Documents/Clear_OS/11_agentes_automacoes/12_integracoes_agent/pipeline/configs/bling_ingest_hub_v1.json --run-id bling_v1_YYYYMMDD
```

2. Gerar migration SQL de importacao:
```powershell
python C:/Users/cesar.zarovski/Documents/Clear_OS/11_agentes_automacoes/12_integracoes_agent/pipeline/06_generate_bling_supabase_import.py --from-date 2025-01-01 --run-id bling_import_v1_YYYYMMDD --batch-size 400
```

3. Aplicar migration no projeto Supabase linkado:
```powershell
$env:SUPABASE_ACCESS_TOKEN=(Get-Content -Raw "$HOME/Documents/token supabase.txt").Trim()
npx.cmd supabase db push --linked --include-all --yes
```

4. Confirmar historico remoto:
```powershell
npx.cmd supabase migration list --linked
```

5. (Opcional) Registrar rotina diaria no Windows Task Scheduler:
```powershell
powershell -ExecutionPolicy Bypass -File C:/Users/cesar.zarovski/Documents/Clear_OS/11_agentes_automacoes/12_integracoes_agent/pipeline/08_register_bling_supabase_daily_task.ps1 -Company CZ -StartTime "06:10" -FromDate "2025-01-01"
```

## Validacoes SQL
```sql
select count(*) as suppliers_bling
from public.suppliers
where source_system = 'bling';

select count(*) as customers_bling
from public.customers
where source_system = 'bling';

select count(*) as ap_bling
from public.accounts_payable
where source_system = 'bling';

select count(*) as ar_bling
from public.accounts_receivable
where source_system = 'bling';

select count(*) as tx_bling
from public.financial_transactions
where source_system = 'bling';
```

## Observacoes
- O processo usa `external_ref` + `source_system` para upsert idempotente.
- Datas sao normalizadas com `LEAST/GREATEST` para respeitar constraints de vencimento/emissao.
- Categorias auto-geradas para carga:
  - `BLING_AP_IMPORT`
  - `BLING_AR_IMPORT`
- Fase 2 (ja aplicada): sincronizacao de `financial_transactions` via AP/AR do Bling.
