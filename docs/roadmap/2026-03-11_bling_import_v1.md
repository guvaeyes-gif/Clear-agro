# 2026-03-11 - Bling Import v1 (Supabase)

## Entregas
- Config de ingestao Bling para skill `finance-ingest-hub`.
- Script gerador de migration incremental Bling -> Supabase.
- Script de execucao diaria e script de registro no Task Scheduler.
- Migration aplicada no projeto remoto:
  - `20260311101915_bling_import_v1.sql`
  - `20260311120000_bling_financial_transactions_v1.sql`

## Fontes usadas
- `contas_pagar_cache.jsonl`
- `contas_receber_cache.jsonl`
- `contatos_cache.jsonl`

## Resultado
- Dados Bling carregados em `suppliers`, `customers`, `accounts_payable` e `accounts_receivable`.
- `financial_transactions` preenchida de forma idempotente a partir de AP/AR do Bling.
- Estrutura preparada para futuras cargas incrementais por `source_system/external_ref`.

## Proximo passo
- Ativar rotina diaria agendada no Windows.
- Incluir reconciliacao com extratos bancarios para status `reconciled` de transacoes.
