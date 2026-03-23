# Migracoes

Diretorio reservado para documentacao da governanca de migrations.

## Regra oficial
- Caminho canonico de migrations executaveis: `supabase/migrations/`
- Execucao local/remota via Supabase CLI deve usar somente essa trilha.
- Nao adicionar novos arquivos `.sql` neste diretorio.

## Historico
- Espelhos legados de migrations antes duplicados neste caminho foram arquivados em `archive/database_migrations_legacy/`.
- O objetivo e eliminar dupla fonte de verdade entre `database/migrations/` e `supabase/migrations/`.

## Migrations canonicas atuais
- `20260311100500_phase1a_core_tables.sql`
- `20260311100600_phase1a_seed_super_admin.sql`
- `20260311100700_phase1a_rls_blueprint.sql`
- `20260311101915_bling_import_v1.sql`
- `20260311102000_fix_super_admin_identity.sql`
- `20260311104000_link_auth_user_cesar.sql`
- `20260311104211_bling_import_v1.sql`
- `20260311113208_bling_import_v1.sql`
- `20260311120000_bling_financial_transactions_v1.sql`
- `20260311145644_bling_import_v1.sql`
- `20260311145949_bling_import_v1.sql`
- `20260311152000_finance_dashboard_v1_views.sql`

Consulte os arquivos reais em `supabase/migrations/`.
