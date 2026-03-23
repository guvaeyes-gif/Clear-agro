# Monitoramento e Observabilidade

## Objetivo
Preparar a base do Clear OS para monitorar integracoes, jobs, agentes e qualidade operacional com foco em rastreabilidade e resposta rapida.

## O que deve ser monitorado
- Integracao Bling por empresa.
- Futuras integracoes de CRM.
- Execucao dos jobs diarios.
- Falhas por job e por empresa.
- Reconciliacao Bling x Supabase.
- Jobs sem rodar na janela prevista.
- Jobs com atraso ou duracao anormal.
- Agentes com erro ou sem status final.
- Volume anormal de registros lidos ou gravados.
- Divergencia entre origem e destino.

## Fontes de dados
| Fonte | Conteudo |
|---|---|
| `logs/integration/runs` | log tecnico de integracao |
| `logs/integration/status` | status JSON e QA CSV |
| `logs/integration/scheduler` | saida do wrapper e agendamento |
| `logs/agents` | execucao de agentes e orquestrador |
| `logs/audit` | auditoria de mudancas e incidentes |
| `logs/audit/lock_events` | aquisicao, bloqueio e liberacao de locks operacionais |
| `out/dashboard_financeiro_v1` | healthcheck e status do publisher do dashboard |

## Indicadores minimos
| Indicador | Tipo | Prioridade |
|---|---|---|
| ultima execucao por job | disponibilidade | alta |
| status final por job e empresa | confiabilidade | alta |
| duracao da execucao | performance | alta |
| contagem lida, gravada e com erro | volume | alta |
| checks PASS/FAIL da reconciliacao | qualidade | alta |
| idade do ultimo status valido | frescor | alta |
| quantidade de retries | estabilidade | media |
| jobs sem log nas ultimas 24h | observabilidade | alta |
| alteracoes em scheduler e integracoes | auditoria | media |

## Alertas recomendados
- Falha em qualquer job diario critico.
- Reconciliacao com `fail_count > 0`.
- Ausencia de status diario para `CZ` ou `CR`.
- `db push` com duracao acima da linha de base.
- Dashboard publisher com `ready = false`.
- Job executando fora da ordem recomendada.
- Lock bloqueado para pipeline, reconciliacao ou `db push`.

## Estrategia inicial
- Curto prazo: consolidar consumo de JSON/CSV e logs em dashboards tecnicos.
- Medio prazo: padronizar emissao de status por todos os jobs transversais.
- Longo prazo: adicionar alertas automatizados e lock operacional com eventos auditaveis.

## Gaps atuais
- Publisher do dashboard usa `out/` e nao a trilha alvo de logs.
- Nao ha status formal de agentes em `logs/agents/status`.
- Ainda nao ha painel consolidado consumindo `logs/audit/lock_events`.

## Recomendacao
Toda nova automacao transversal deve publicar status no formato padronizado e registrar contexto suficiente para alimentar dashboards tecnicos sem parser customizado por job.
