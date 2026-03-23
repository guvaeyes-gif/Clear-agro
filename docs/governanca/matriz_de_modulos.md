# Matriz de Modulos do Clear OS

## Objetivo
Definir fronteiras de leitura, escrita, ownership e nivel de sensibilidade para reduzir conflito entre dominios e agentes.

| Modulo | Papel principal | Leitura permitida | Escrita permitida | Owner primario | Sensibilidade |
|---|---|---|---|---|---|
| `database` | schema, views, migrations, QA | todos os modulos tecnicos | apenas trilha versionada de banco | Dados/Engenharia | alta |
| `integrations/bling` | extracao, ingestao, carga e reconciliacao Bling | configs, caches, supabase, logs | status, migrations geradas, artefatos de reconciliacao | Integracoes | alta |
| `integrations/shared` | contratos reutilizaveis e utilitarios de integracao | leitura ampla | escrita controlada por Plataforma | Plataforma | media |
| `automation/jobs` | wrappers de execucao | runners e scripts canonicos | wrappers e comandos de disparo | Plataforma | alta |
| `automation/scheduler` | registro de tarefas agendadas | jobs, runners, logs | scripts de registro | Plataforma | alta |
| `automation/scripts` | compatibilidade transitoria e utilitarios de execucao | runners, configs, logs | apenas scripts auxiliares aprovados | Plataforma | media |
| `logs/integration` | trilha tecnica de integracoes | leitura ampla | apenas jobs e runners | Plataforma/Integracoes | alta |
| `logs/agents` | trilha tecnica de agentes | leitura ampla | apenas agentes autorizados | Plataforma/Operacoes | alta |
| `logs/audit` | auditoria operacional e mudancas | leitura ampla | apenas processos de governanca | Governanca/Plataforma | alta |
| `docs/arquitetura` | mapa tecnico e desenho operacional | leitura ampla | documentacao arquitetural | Plataforma | media |
| `docs/governanca` | regras operacionais e ownership | leitura ampla | documentacao de governanca | Governanca/Plataforma | alta |
| `docs/processos` | diagnosticos, inventarios e transicao | leitura ampla | documentacao operacional | Plataforma | media |
| `docs/runbooks` | operacao e troubleshooting | leitura ampla | documentacao operacional | Operacoes/Plataforma | media |
| `dashboards/specs` | especificacoes de paineis | leitura ampla | specs aprovadas | BI/Plataforma | media |
| `security` | controles, politicas, acesso e auditoria | leitura ampla controlada | somente governanca autorizada | Governanca | alta |
| `agents/crm` | operacao do dominio CRM | interfaces compartilhadas, logs, config | apenas dentro do proprio dominio | CRM | alta |
| `agents/financeiro` | operacao do dominio financeiro | interfaces compartilhadas, logs, config | apenas dentro do proprio dominio | Financeiro | alta |
| `agents/operacoes` | observabilidade, alertas e consolidacao operacional | logs, status, docs | apenas artefatos operacionais aprovados | Operacoes | media |
| `dashboards` | consultas e especificacoes de BI | views e status validados | artefatos de painel e specs | BI | media |

## Regras de fronteira
- CRM nao altera logica funcional de Financeiro.
- Financeiro nao altera contratos de CRM.
- Integracoes podem ler dados de dominio, mas so escrevem por contrato explicito.
- `automation` nao deve duplicar logica de negocio; deve apenas orquestrar.
- `logs` sao area compartilhada e sensivel; escrita somente por processo autorizado.
- `security`, `database/migrations` e `automation/scheduler` exigem aprovacao antes de mudanca.

## Regras para multiagentes
- `docs`, `dashboards/specs` e `runbooks` podem ser trabalhados em paralelo com baixo risco, desde que cada agente tenha escopo claro.
- `integrations/bling`, `automation/scheduler`, `database/migrations` e `security` exigem isolamento por branch ou worktree.
- Pastas legadas numeradas devem ser tratadas como dependencia operacional ou historica; alteracao so com registro de impacto.

## Fonte canonica operacional
- Runner diario Bling: `integrations/bling/runners/run_bling_supabase_daily.ps1`
- Registro de task Bling: `integrations/bling/runners/register_bling_supabase_daily_task.ps1`
- Wrappers de scheduler: `automation/jobs`
- Ponto de observabilidade alvo: `logs/integration`, `logs/agents`, `logs/audit`
