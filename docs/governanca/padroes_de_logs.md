# Padroes de Logs e Status Operacionais

## Objetivo
Estabelecer um contrato minimo unico para logs, status e auditoria do Clear OS sem quebrar os fluxos produtivos ja ativos.

## Estado atual encontrado
- O runner canonico de Bling grava logs tecnicos em `logs/integration/runs`.
- O scheduler grava saida de tarefa em `logs/integration/scheduler`.
- O ingest hub e a reconciliacao publicam status estruturado em `logs/integration/status`.
- Ainda existem referencias legadas a `11_agentes_automacoes/12_integracoes_agent/pipeline/out/status` em scripts e configs antigos.
- O dashboard publisher grava artefatos tecnicos em `out/dashboard_financeiro_v1`, fora do padrao alvo de observabilidade.

## Principios
- Todo job deve gerar um identificador unico por execucao.
- Log tecnico, status estruturado e trilha de auditoria nao devem ser misturados no mesmo arquivo.
- O nome do campo canonico passa a ser `execution_id`.
- Quando houver compatibilidade com legado, aceitar `run_id` como alias documental de `execution_id`.
- Todo status deve informar origem, destino, contexto de empresa e resultado final.
- Toda falha deve ser rastreavel sem depender de leitura manual do log inteiro.

## Tipos de artefato
| Tipo | Objetivo | Local padrao | Formato |
|---|---|---|---|
| Log tecnico de execucao | Linha a linha da rodada | `logs/integration/runs`, `logs/agents/runs` | `.log` |
| Status estruturado | Resumo da execucao e metrica principal | `logs/integration/status`, `logs/agents/status` | `.json` |
| QA operacional | Resultado de reconciliacao, checks e gates | `logs/integration/status`, `logs/agents/status` | `.csv` |
| Auditoria | Registro de mudanca, aprovacao e incidente | `logs/audit` | `.json` ou `.md` |
| Log de scheduler | Saida de wrapper e task agendada | `logs/integration/scheduler` | `.log` |
| Evento de lock | Aquisicao, bloqueio e liberacao de recurso | `logs/audit/lock_events` | `.json` |

## Campos obrigatorios do status
| Campo | Obrigatorio | Observacao |
|---|---|---|
| `execution_id` | sim | Usar o mesmo valor em todos os artefatos da rodada |
| `run_id` | recomendado | Alias temporario para compatibilidade com scripts atuais |
| `job_name` | sim | Nome do job ou wrapper executado |
| `module_name` | sim | Ex.: `integrations/bling`, `dashboards/financeiro`, `agents/operacoes` |
| `source_system` | sim | Ex.: `bling`, `supabase`, `manual`, `metabase` |
| `target_system` | sim | Ex.: `logs`, `supabase`, `dashboard`, `status_store` |
| `company_code` | quando aplicavel | `CZ`, `CR`, `ALL` ou `null` |
| `started_at` | sim | ISO 8601 |
| `finished_at` | sim | ISO 8601 |
| `duration_ms` | sim | Tempo total em milissegundos |
| `status` | sim | `success`, `partial`, `failed`, `warning`, `skipped` |
| `records_read` | recomendado | Quantidade lida da origem |
| `records_written` | recomendado | Quantidade gravada no destino |
| `records_failed` | recomendado | Quantidade com erro |
| `error_code` | quando falha | Codigo curto e pesquisavel |
| `error_message` | quando falha | Mensagem curta e objetiva |
| `payload_ref` | recomendado | Caminho de arquivo, migration, query ou lote |
| `triggered_by` | sim | `scheduler`, `manual`, `agent`, `ci` |
| `environment` | sim | `local`, `dev`, `prod`, `unknown` |

## Campos recomendados para auditoria
- `change_type`
- `approved_by`
- `related_module`
- `risk_level`
- `rollback_reference`
- `evidence_ref`

## Convencoes de nomenclatura
- Log tecnico: `<job_name>_<execution_id>.log`
- Status: `<job_name>_<execution_id>_status.json`
- QA: `<job_name>_<execution_id>_qa.csv`
- Auditoria: `<change_type>_<execution_id>.json`

## Niveis de severidade
| Campo | Uso |
|---|---|
| `INFO` | inicio, fim, contagem, contexto |
| `WARN` | retry, degradacao, fallback, dado parcial |
| `ERROR` | falha de passo, dependencia ausente, validacao critica |
| `AUDIT` | aprovacao, alteracao estrutural, lock, incidente |

## Regras de publicacao
- Um job pode ter multiplos logs tecnicos, mas deve publicar um unico status final.
- Retries devem manter o mesmo `execution_id` da rodada e aumentar `retry_count` quando houver esse campo.
- Status parcial deve existir quando a execucao termina com saidas uteis, mas com warnings ou checks falhos.
- Se houver QA CSV, o status final deve apontar para ele em `payload_ref` ou campo equivalente.

## Estrutura recomendada de pastas
- `logs/integration/runs`
- `logs/integration/status`
- `logs/integration/scheduler`
- `logs/agents/runs`
- `logs/agents/status`
- `logs/audit`
- `logs/audit/locks`
- `logs/audit/lock_events`

## Compatibilidade com o estado atual
- O runner `integrations/bling/runners/run_bling_supabase_daily.ps1` ja escreve em `logs/integration/runs` e `logs/integration/status`; ele passa a ser a referencia para novos jobs.
- O gerador de migration e a reconciliacao ainda publicam `run_id`; o padrao novo aceita esse campo enquanto a transicao nao for automatizada.
- O `scripts/finance_dashboard_publisher.py` continua escrevendo em `out/dashboard_financeiro_v1`, mas os proximos jobs tecnicos devem espelhar status final em `logs/agents/status` ou `logs/integration/status`.

## Templates
Templates de referencia foram adicionados em `config/templates` para padronizar novos jobs sem alterar scripts atuais.
