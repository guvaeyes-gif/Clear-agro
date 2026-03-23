# Regras de Execucao Paralela

## Objetivo
Definir o que pode rodar em paralelo, o que exige lock e o que precisa de branch ou worktree isolada.

## Niveis de risco
| Nivel | Definicao |
|---|---|
| baixo | impacto apenas documental ou leitura |
| medio | escrita em artefato compartilhado sem efeito produtivo imediato |
| alto | escrita em scheduler, banco, logs compartilhados ou integracao produtiva |

## Pode rodar em paralelo com baixo risco
- Atualizacao de docs em arquivos diferentes.
- Edicao de specs em `dashboards/specs`.
- Leitura de `database`, `agents` e `dashboards` para diagnostico.
- Elaboracao de runbooks em arquivos distintos.

## Pode rodar em paralelo com controle
- Runners Bling por empresa, desde que exista lock global para `db push`.
- Geracao de status e QA em arquivos com `execution_id` unico.
- Analise de logs por agentes diferentes, desde que em modo leitura.

## Nao pode rodar em paralelo
- Dois `supabase db push` para o mesmo ambiente.
- Dois registradores de scheduler alterando a mesma task.
- Dois agentes alterando o mesmo arquivo de governanca critica ao mesmo tempo.
- Reconciliacao e mutacao de schema na mesma janela sem controle.

## Locks recomendados
| Recurso | Tipo de lock | Granularidade |
|---|---|---|
| Runner Bling | lock logico | `job_name + company_code + data` |
| `db push` | lock global | ambiente |
| Reconciliacao | lock logico | `company_code + from_date` |
| Scheduler | lock administrativo | nome da task |
| Dashboard publisher | lock leve | painel e janela de publicacao |

## Estado implementado
- Runner diario Bling com lock por empresa: `bling_pipeline_<company>`.
- `supabase db push` com lock global: `supabase_db_push`.
- Reconciliacao com lock por escopo de empresa: `bling_reconciliation_<company_scope>`.
- Eventos de lock gravados em `logs/audit/lock_events`.
- Locks ativos mantidos em `logs/audit/locks`.

## Worktree ou branch obrigatoria
- `database/migrations`
- `automation/scheduler`
- `integrations/bling/runners`
- `security`
- qualquer alteracao que mexa em contrato compartilhado entre agentes

## Regras para agentes
- Agente de CRM nao altera runner ou scheduler de integracao.
- Agente Financeiro nao altera ownership ou governanca transversal sem registro.
- Agente de Operacoes pode ler todos os status, mas so escreve em areas operacionais aprovadas.
- Alteracao em modulo compartilhado deve declarar impacto em `docs/governanca` ou `docs/processos`.

## Regra pratica atual
Mesmo com lock minimo implementado, manter operacao serial como padrao seguro para mudancas estruturais, scheduler e qualquer escrita critica fora do pipeline Bling principal.
