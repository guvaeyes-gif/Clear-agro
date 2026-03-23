# Matriz Canonica de Caminhos

## Objetivo
Definir, de forma explicita, qual caminho deve ser tratado como fonte de verdade em cada modulo critico do Clear OS, quem pode altera-lo e qual trilha e apenas compatibilidade transitoria.

## Como interpretar este documento
- `canonico operacional atual`: caminho que deve ser usado hoje para operar sem quebrar o que ja funciona.
- `compatibilidade`: caminho legado, espelho ou wrapper preservado para nao romper automacoes, app ou rotina humana.
- `alvo de convergencia`: caminho desejado para consolidacao futura, sem implicar mudanca imediata.
- Em caso de conflito entre docs antigos, este documento prevalece para decisao operacional.

## Regras gerais
- Nao alterar `compatibilidade` por suposicao. Primeiro validar se ha scheduler, app, script ou agente ainda apontando para esse caminho.
- Modulos com sensibilidade `alta` exigem branch ou worktree isolada e leitura previa de governanca.
- `automation` pode disparar jobs, mas nao deve concentrar logica de negocio.
- `logs` sao contrato operacional, nao area livre para escrita manual.

## Matriz principal
| Dominio | Artefato | Canonico operacional atual | Compatibilidade / legado | Alvo de convergencia | Owner primario | Sensibilidade | Regra de mudanca |
|---|---|---|---|---|---|---|---|
| Integracao Bling | Runner diario | `integrations/bling/runners/run_bling_supabase_daily.ps1` | `automation/scripts/run_bling_supabase_daily.ps1`, `11_agentes_automacoes/12_integracoes_agent/pipeline/07_run_bling_supabase_daily.ps1`, wrappers na raiz | manter em `integrations/bling/runners/` | Plataforma + Integracoes | alta | aprovar antes de alterar |
| Integracao Bling | Registro do scheduler | `integrations/bling/runners/register_bling_supabase_daily_task.ps1` | `automation/scheduler/register_bling_supabase_daily_task.ps1`, `11_agentes_automacoes/12_integracoes_agent/pipeline/08_register_bling_supabase_daily_task.ps1` | manter em `integrations/bling/runners/` | Plataforma | alta | aprovar antes de alterar |
| Automacao | Wrapper diario `CZ` | `automation/jobs/run_bling_supabase_daily_cz.cmd` | `run_bling_supabase_daily_cz.cmd` | manter em `automation/jobs/` | Plataforma | alta | nao reapontar sem janela controlada |
| Automacao | Wrapper diario `CR` | `automation/jobs/run_bling_supabase_daily_cr.cmd` | `run_bling_supabase_daily_cr.cmd` | manter em `automation/jobs/` | Plataforma | alta | nao reapontar sem janela controlada |
| Automacao | Publicador do dashboard | `automation/jobs/run_finance_dashboard_publisher.cmd` | execucao manual direta do script | manter em `automation/jobs/` | BI + Plataforma | media | alterar junto com runbook |
| Banco | Migrations executaveis | `supabase/migrations/` | `10_banco_de_dados/migracoes/`, docs antigas, espelhos em repos aninhados | manter em `supabase/migrations/` | Dados/Engenharia | alta | nunca editar migration aplicada |
| Banco | Config local Supabase | `supabase/config.toml` | nenhum equivalente valido | manter em `supabase/` | Dados/Engenharia | alta | alterar com impacto registrado |
| Integracao Bling | Gerador de carga | `integrations/bling/load/generate_bling_supabase_import.py` | `11_agentes_automacoes/12_integracoes_agent/pipeline/06_generate_bling_supabase_import.py` | manter em `integrations/bling/load/` | Integracoes | alta | validar com status/logs |
| Integracao Bling | Reconciliacao | `integrations/bling/reconciliation/reconcile_bling_supabase.py` | `11_agentes_automacoes/12_integracoes_agent/pipeline/09_reconcile_bling_supabase.py` | manter em `integrations/bling/reconciliation/` | Integracoes + Financeiro | alta | alterar com smoke test e leitura dos status |
| Observabilidade | Logs de integracao | `logs/integration/` | `11_agentes_automacoes/12_integracoes_agent/pipeline/out/logs/` | manter em `logs/integration/` | Operacoes + Plataforma | alta | nao quebrar formato dos status |
| Observabilidade | Status de integracao | `logs/integration/status/` | `11_agentes_automacoes/12_integracoes_agent/pipeline/out/status/` | manter em `logs/integration/status/` | Operacoes + Integracoes | alta | preservar compatibilidade enquanto houver consumidores |
| Observabilidade | Auditoria de locks | `logs/audit/lock_events/` e `logs/audit/locks/` | nenhum | manter em `logs/audit/` | Plataforma | alta | escrita apenas por processo autorizado |
| Dashboards | SQL Metabase financeiro | `dashboards/metabase/` | `12_dashboards_interface/painel_financeiro/metabase/sql_cards/` | manter em `dashboards/metabase/` | BI | media | alterar junto com spec e runbook |
| Dashboards | Artefatos de publicacao | `out/dashboard_financeiro_v1/` | nenhum consolidado em `logs/` ainda | publicar em `out/` e futuramente espelhar status em `logs/` | BI + Operacoes | media | manter compatibilidade com runbook atual |
| CRM App | Entry point Streamlit | `app/main.py` | `app/gestor.py`, `app/diretor.py` como perfis auxiliares | manter `app/main.py` | CRM + Plataforma | media | validar startup local |
| CRM App | Repositorio de metas / CRM | `src/metas_db.py` | `src/metas_db_sqlite_legacy.py` | convergir para Postgres/Supabase sem fallback silencioso | CRM + Dados | alta | transicao gradual, sem quebrar app |

## Matriz de fontes de dados
| Grupo de dados | Uso principal | Canonico operacional atual | Compatibilidade / legado | Alvo de convergencia | Owner primario | Risco atual |
|---|---|---|---|---|---|---|
| Bling AP, AR e contatos | pipeline financeiro produtivo | `config/paths/bling_cache_roots.json` com raiz homologada em `bling_api/` e fallback para a raiz aninhada | `11_agentes_automacoes/11_dev_codex_agent/repos/CRM_Clear_Agro/bling_api/` durante a transicao | consolidar tudo na raiz homologada `bling_api/` | Integracoes | alto |
| Bling vendas, NFe e mapa de vendedores | app CRM e relatorios locais | `config/paths/bling_cache_roots.json` com raiz homologada em `bling_api/` | copia parcial em repo aninhado | consolidar em um unico cache versionado por dominio | CRM + Integracoes | medio |
| Base comercial consolidada | app Streamlit | `out/base_unificada.xlsx` | `out/base_unificada_gestor.xlsx` e SQLite legado | substituir por views/tabelas CRM no Supabase | CRM | medio |
| Token Supabase local | `db push`, REST e reconciliacao | arquivo local do usuario fora do repo, referenciado por scripts | variavel `SUPABASE_ACCESS_TOKEN` | cofre local ou variavel de ambiente padronizada | Plataforma + Dados | alto |
| Credenciais Bling | sync ERP | arquivo local em `00_governanca/politicas_de_acesso/legacy_credenciais/` | outras copias locais nao homologadas | cofre local fora do workspace compartilhado | Governanca + Integracoes | alto |

## Matriz de ownership por tipo de escrita
| Area | Escrita permitida | Escrita proibida sem aprovacao |
|---|---|---|
| Plataforma | `automation/jobs`, `logs/audit`, wrappers, contratos compartilhados | `supabase/migrations`, segredos, scheduler em producao sem janela |
| Integracoes | `integrations/bling`, configs de pipeline, status de integracao | wrappers de scheduler, politicas de acesso, contratos CRM |
| Financeiro | reconciliacao, QA, views e specs financeiras aprovadas | runner Bling, scheduler, segredos |
| CRM | `app/`, `src/`, `integrations/crm`, seeds e views CRM | runner financeiro, task scheduler, logs compartilhados fora do contrato |
| BI | `dashboards/metabase`, `dashboards/specs`, artefatos em `out/` | paths de integracao e schema produtivo |
| Operacoes | consumo de `logs/`, runbooks, alertas e consolidacao | alteracao de logica de negocio em integracoes |

## Decisoes praticas para o estado atual
### 1. Scheduler e disparo
- O scheduler deve mirar wrappers em `automation/jobs/`.
- Os wrappers da raiz existem apenas como legado e nao devem receber novos ajustes.

### 2. Bling
- A raiz fisica homologada passa a ser `bling_api/` na raiz do workspace.
- A raiz aninhada em `11_agentes_automacoes/.../CRM_Clear_Agro/bling_api/` continua como compatibilidade e fonte de sincronizacao controlada.
- Enquanto a homologacao nao estiver concluida, o fallback automatico continua permitido.

### 3. Banco e dashboards
- Toda mutation de schema executavel pertence a `supabase/migrations/`.
- Todo SQL de Metabase que entra em producao deve existir em `dashboards/metabase/`.
- `12_dashboards_interface/` deve ser tratado como legado ou apoio visual, nao como fonte de verdade do SQL.

### 4. Logs e status
- `logs/integration/status/` e a trilha oficial para jobs de integracao.
- `out/` continua valido para artefatos de publicacao e app, mas nao deve virar substituto de observabilidade transversal.

## Checklist antes de alterar um caminho canonico
1. Confirmar quem consome o caminho hoje.
2. Confirmar se existe scheduler, wrapper, app ou agente apontando para ele.
3. Registrar impacto e rollback.
4. Validar logs, status e runbook afetados.
5. Atualizar este documento se a fonte de verdade mudar.

## Pendencias declaradas
- Consolidar os caches Bling em uma unica raiz homologada.
- Desativar operacionalmente os wrappers da raiz apos validacao controlada.
- Fazer o publisher do dashboard espelhar status final em `logs/`.
- Remover dependencia operacional de fallback SQLite no CRM.
