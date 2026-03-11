# Inventario Final - Reorganizacao Clear OS (2026-03-11)

## 1) Arvore final (estrutura padrao)
- `docs/{arquitetura,roadmap,processos,governanca,runbooks}`
- `database/{migrations,schema,views,functions,seeds,qa}`
- `integrations/{bling,sheets,crm,shared}`
- `agents/{orchestrator,financeiro,crm,compras,operacoes,shared,prompts}`
- `dashboards/{metabase,grafana,specs}`
- `automation/{jobs,scheduler,scripts}`
- `logs/{integration,agents,audit}`
- `config/{env,templates}`
- `security/{access,policies,audit}`
- `tests/{integration,database,agents}`
- `archive/`

## 2) Arquivos reposicionados (copia segura)
- Roadmap:
  - `00_governanca/roadmap_clear_os/*` -> `docs/roadmap/`
- Runbooks:
  - `13_logs_monitoramento/runbook_dashboard_financeiro_v1.md` -> `docs/runbooks/dashboard_financeiro_v1_runbook.md`
  - `10_banco_de_dados/documentacao_sql/bling_import_v1_runbook.md` -> `docs/runbooks/bling_import_v1_runbook.md`
- Database:
  - `10_banco_de_dados/migracoes/*.sql` -> `database/migrations/`
  - `supabase/migrations/*.sql` -> `database/migrations/`
- Integrations Bling:
  - `pipeline/06_generate_bling_supabase_import.py` -> `integrations/bling/load/generate_bling_supabase_import.py`
  - `pipeline/09_reconcile_bling_supabase.py` -> `integrations/bling/reconciliation/reconcile_bling_supabase.py`
  - `pipeline/07_run_bling_supabase_daily.ps1` -> `integrations/bling/runners/run_bling_supabase_daily.ps1`
  - `pipeline/08_register_bling_supabase_daily_task.ps1` -> `integrations/bling/runners/register_bling_supabase_daily_task.ps1`
  - `pipeline/configs/bling_ingest_hub_v1.json` -> `integrations/bling/config/bling_ingest_hub_v1.json`
- Dashboards:
  - `12_dashboards_interface/painel_financeiro/metabase/sql_cards/*.sql` -> `dashboards/metabase/`
  - `12_dashboards_interface/painel_financeiro/README_dashboard_financeiro_v1.md` -> `dashboards/specs/dashboard_financeiro_v1.md`
- Automation:
  - `run_bling_supabase_daily_{cz,cr}.cmd` -> `automation/jobs/`
  - `pipeline/08_register_bling_supabase_daily_task.ps1` -> `automation/scheduler/`
  - `pipeline/07_run_bling_supabase_daily.ps1` -> `automation/scripts/`

## 3) Arquivos criados
- Git:
  - `.gitignore`
  - `README.md`
  - `CHANGELOG.md`
  - `CONTRIBUTING.md`
- Governanca:
  - `docs/governanca/branching_strategy.md`
  - `docs/governanca/regras_de_mudanca.md`
  - `docs/governanca/padroes_de_logs.md`
  - `docs/governanca/padroes_de_nomenclatura.md`
  - `docs/governanca/matriz_de_modulos.md`
- Arquitetura:
  - `docs/arquitetura/arquitetura_multiagentes.md`
- Agentes:
  - `agents/orchestrator/README.md`
  - `agents/financeiro/README.md`
  - `agents/crm/README.md`
  - `agents/compras/README.md`
  - `agents/operacoes/README.md`
- Processos:
  - `docs/processos/diagnostico_estrutura_2026-03-11.md`
  - `docs/processos/transicao_legacy_para_estrutura_padrao.md`

## 4) Riscos identificados
- Scheduler ainda depende de caminhos legacy de producao.
- Scripts espelhados podem divergir se nao houver politica de "fonte canonica".
- Volume de arquivos legados (incluindo caches/node_modules) aumenta ruido no versionamento.
- Credenciais legadas exigem reforco de segregacao e cofres de segredo.

## 5) Pendencias para proxima sprint
- Definir oficialmente fonte canonica dos scripts Bling (legacy vs estrutura nova).
- Mover scheduler para caminhos novos (`integrations/bling/runners`) com janela controlada.
- Limpar e arquivar artefatos legados nao operacionais.
- Ativar pipeline de CI para checks minimos (lint/test/schema).
- Definir processo de release para dashboards (promocao dev -> prod).

## 6) Pronto para
- Git: pronto (repo inicializado, governanca e ignore basicos criados).
- Dashboards: pronto para publicacao controlada no Metabase (cards + runbook + gate).
- Novas integracoes: base padronizada criada em `integrations/`.
- Agentes: estrutura e contratos iniciais prontos em `agents/`.
- Producao futura OpenClaw: base governada pronta para evolucao incremental.

