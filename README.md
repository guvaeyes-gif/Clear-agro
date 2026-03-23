# Clear OS

Workspace oficial da Clear Agro para dados, integracoes, dashboards e operacao futura multiagentes.

## Objetivo
- Governanca de dados e automacao com trilha de auditoria.
- Integracao ERP (Bling) + Supabase com cargas diarias.
- Dashboards setoriais com gate de qualidade.
- Base escalavel para agentes especializados.

## Estrutura principal (padrao)
- `docs/`: arquitetura, roadmap, processos, governanca e runbooks.
- `database/`: schema, views, functions, seeds, QA e documentacao da camada de dados.
- `supabase/`: configuracao local e trilha canonica de migrations SQL executaveis.
- `integrations/`: conectores e pipelines (Bling, Sheets, CRM, shared).
- `agents/`: definicao de agentes por dominio e orquestrador.
- `dashboards/`: assets e specs de BI (Metabase/Grafana).
- `automation/`: jobs, scheduler e scripts operacionais.
- `logs/`: trilhas de execucao e auditoria.
- `config/`: templates de configuracao e env.
- `security/`: politicas, acessos e auditoria.
- `tests/`: suites de validacao (integration, database, agents).
- `archive/`: legado e transicao.

## Governanca de migrations
- Caminho canonico de migrations executaveis: `supabase/migrations/`
- `database/migrations/` passa a ser apenas documentacao/compatibilidade.
- Espelhos SQL antigos foram arquivados em `archive/database_migrations_legacy/` para referencia historica.

## Estrutura legacy preservada
A estrutura numerada (`00_governanca` ... `14_documentacao_corporativa`) foi mantida para nao quebrar operacao atual.

Scripts produtivos atuais continuam ativos em:
- `11_agentes_automacoes/12_integracoes_agent/pipeline/`
- `run_bling_supabase_daily_cz.cmd`
- `run_bling_supabase_daily_cr.cmd`

## Navegacao rapida
- Arquitetura multiagentes: `docs/arquitetura/arquitetura_multiagentes.md`
- Roadmap atual: `docs/roadmap/`
- Governanca de mudanca: `docs/governanca/regras_de_mudanca.md`
- Matriz canonica de caminhos: `docs/governanca/matriz_canonica_de_caminhos.md`
- Estrategia de branches: `docs/governanca/branching_strategy.md`
- Runbook dashboard financeiro: `docs/runbooks/dashboard_financeiro_v1_runbook.md`
- Deploy publico CRM review: `docs/runbooks/render_crm_public_review.md`
