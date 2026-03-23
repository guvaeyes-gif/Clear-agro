# Database

Camada de dados do Clear OS:
- schema
- views
- functions
- seeds
- qa
- documentacao e referencia de governanca

## Regra de governanca
- Migrations SQL executaveis vivem exclusivamente em `supabase/migrations/`.
- `database/migrations/` nao deve mais receber arquivos `.sql` ativos.
- Espelhos antigos foram movidos para `archive/database_migrations_legacy/`.
