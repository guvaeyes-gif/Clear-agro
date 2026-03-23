# Governanca de Migrations SQL

## Decisao
A trilha canonica e unica de migrations executaveis do Clear OS e:
- `supabase/migrations/`

## Motivacao
Existia duplicacao entre `database/migrations/` e `supabase/migrations/`, incluindo divergencia real de conteudo em arquivos com o mesmo nome. Isso criava dupla fonte de verdade e risco de drift de schema.

## Regras
- Novas migrations `.sql` devem ser criadas apenas em `supabase/migrations/`.
- Execucoes via Supabase CLI devem usar apenas a pasta `supabase/`.
- `database/migrations/` fica reservado para documentacao de governanca e compatibilidade.
- Espelhos antigos devem ser mantidos somente em `archive/database_migrations_legacy/` para referencia historica.

## Impacto operacional
- Runbooks, templates e checagens devem apontar para `supabase/migrations/`.
- Qualquer artefato que dependa de paths de migrations deve ser atualizado para o caminho canonico.

## Validacao minima
1. `supabase/migrations/` contem todas as migrations executaveis.
2. `database/migrations/` nao contem arquivos `.sql` ativos.
3. Templates e runbooks apontam para `supabase/migrations/`.

## Comando de validacao
- make check-migration-governance`n- python scripts/check_migration_governance.py`n
