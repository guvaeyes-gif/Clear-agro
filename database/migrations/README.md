# Migracoes

Registrar scripts de migracao e historico de execucao.

## Migrations existentes

- `20260310_001_phase1a_core_tables.sql`
  - Cria as 10 tabelas base da phase 1A.
  - Cria enums de status e funcao `set_updated_at()`.
  - Cria indices principais para financeiro, CRM e compras.

- `20260310_002_phase1a_seed_super_admin.sql`
  - Seed inicial do usuario `super_admin` no `app_users` a partir de `auth.users`.

- `20260311_003_fix_super_admin_identity.sql`
  - Corrige seed do super_admin com login real no Supabase Auth (`czarovski@gmail.com`).
  - Mantem email corporativo em `metadata.business_email` (`cesar@clearagro.com.br`).

- `20260311_004_link_auth_user_cesar.sql`
  - Vincula `auth.users(czarovski@gmail.com)` ao `app_users(cesar@clearagro.com.br)`.

- `20260311101915_bling_import_v1.sql`
  - Primeira carga Bling (contatos, contas a pagar e contas a receber) para tabelas core.
  - Adiciona `source_system` e `external_ref` em `suppliers`, `customers`, `accounts_payable` e `accounts_receivable`.
  - Cria indices unicos para upsert idempotente por origem externa.

- `20260311_005_bling_financial_transactions_v1.sql`
  - Gera/atualiza `financial_transactions` a partir de `accounts_payable` e `accounts_receivable` da origem Bling.
  - Usa `external_ref` para upsert idempotente de transacoes.

- `20260311152000_finance_dashboard_v1_views.sql`
  - Cria views analiticas para o dashboard financeiro v1 (Metabase):
    - `vw_finance_kpis_daily`
    - `vw_finance_ap_aging`
    - `vw_finance_ar_aging`
    - `vw_finance_cash_projection_30d`
    - `vw_finance_data_quality_banner`
  - Inclui regra de gate de qualidade `PASS/FAIL` com recencia e cobertura `CZ/CR`.
