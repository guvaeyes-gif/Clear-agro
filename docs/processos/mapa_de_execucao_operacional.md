# Mapa de Execucao Operacional

## Objetivo
Explicar como os jobs do Clear OS sao disparados hoje, em que ordem devem rodar e quais dependencias precisam ser respeitadas.

## Jobs identificados
| Job | Tipo | Disparo | Frequencia conhecida | Escopo |
|---|---|---|---|---|
| `run_bling_supabase_daily_cz.cmd` | wrapper | scheduler e manual | diaria 06:10 | empresa `CZ` |
| `run_bling_supabase_daily_cr.cmd` | wrapper | scheduler e manual | diaria 06:25 | empresa `CR` |
| `run_finance_dashboard_publisher.cmd` | wrapper | manual | sob demanda | dashboard financeiro |
| `integrations/bling/runners/run_bling_supabase_daily.ps1` | runner canonico | invocado por wrapper | diaria e manual | pipeline Bling |
| `integrations/bling/runners/register_bling_supabase_daily_task.ps1` | registrador | manual administrativo | eventual | scheduler |

## Ordem recomendada de execucao
1. Sincronizar Bling para o cache local da empresa.
2. Rodar ingest hub com config da empresa.
3. Gerar migration incremental Bling -> Supabase.
4. Aplicar `supabase db push`.
5. Rodar reconciliacao Bling x Supabase.
6. Consultar status e QA antes de qualquer publicacao de dashboard.
7. Publicar ou atualizar dashboard somente apos gate tecnico verde.

## Dependencias por job
| Job | Depende de | Nao pode iniciar sem |
|---|---|---|
| Runner Bling diario | cache, config, credencial Bling, token Supabase | acesso ao repo legado de cache e token local |
| Geracao de migration | cache Bling e `supabase/migrations` | arquivos JSONL e pasta de migrations acessivel |
| Reconciliacao | Supabase acessivel, API key resolvida, cache Bling | token e CLI do Supabase |
| Dashboard publisher | status de reconciliacao, specs SQL e runbook | healthcheck minimo disponivel |

## Jobs manuais
- Registro de task do scheduler.
- Publicacao do dashboard financeiro.
- Execucoes de troubleshooting ou rerun controlado.

## Jobs automaticos
- `ClearOS-Bling-Supabase-Daily-CZ`
- `ClearOS-Bling-Supabase-Daily-CR`

## Paralelismo
- `CZ` e `CR` podem rodar em paralelo apenas se houver lock em `supabase db push` e garantia de nao concorrencia na aplicacao de migrations.
- Sem lock explicito, manter execucao serializada por janela de horario.
- Dashboard publisher nao deve rodar durante `db push` ou reconciliacao em andamento.

## Ponto de entrada canonico
- Runner diario: `integrations/bling/runners/run_bling_supabase_daily.ps1`
- Wrapper de disparo: `automation/jobs/run_bling_supabase_daily_<empresa>.cmd`
- Registro de scheduler: `integrations/bling/runners/register_bling_supabase_daily_task.ps1`

## Inconsistencias encontradas
- `automation/scripts/run_bling_supabase_daily.ps1` foi mantido apenas como shim de compatibilidade e nao deve ser tratado como fonte canonica.
- Existem wrappers legados na raiz do repositorio apontando para o pipeline antigo.
- O dashboard publisher ainda grava status em `out/dashboard_financeiro_v1`, fora da trilha alvo de logs operacionais.

## Recomendacao operacional
- Manter o runner canonico novo como fonte de verdade.
- Usar `automation/jobs` apenas como camada de chamada.
- Tratar `automation/scripts` e `automation/scheduler` apenas como compatibilidade transitora.
- Planejar migracao controlada dos consumidores de status legados antes de remover referencias antigas.
