# Jobs e Runners do Clear OS

## Objetivo
Oferecer uma referencia curta para identificar qual arquivo dispara cada fluxo operacional e onde atuar em caso de incidente.

## Fonte canonica
| Tipo | Caminho | Observacao |
|---|---|---|
| Runner Bling diario | `integrations/bling/runners/run_bling_supabase_daily.ps1` | principal fluxo produtivo |
| Registro do scheduler Bling | `integrations/bling/runners/register_bling_supabase_daily_task.ps1` | registra task no Windows |
| Wrappers de job | `automation/jobs` | camada de disparo e compatibilidade |
| Compatibilidade transitora | `automation/scripts` e `automation/scheduler` | delegam para a trilha canonica |
| Publicador do dashboard | `automation/jobs/run_finance_dashboard_publisher.cmd` | job manual |

## Jobs mapeados
| Job | Runner alvo | Logs principais | Status principal |
|---|---|---|---|
| `run_bling_supabase_daily_cz.cmd` | runner Bling com `CZ` | `logs/integration/scheduler/task_runner_cz.log` | `logs/integration/status` |
| `run_bling_supabase_daily_cr.cmd` | runner Bling com `CR` | `logs/integration/scheduler/task_runner_cr.log` | `logs/integration/status` |
| `run_finance_dashboard_publisher.cmd` | `scripts/finance_dashboard_publisher.py` | console local e `out/dashboard_financeiro_v1` | `out/dashboard_financeiro_v1/*_status.json` |

## Como localizar falha rapidamente
1. Verificar wrapper executado em `automation/jobs`.
2. Confirmar se o runner canonico alvo esta em `integrations/bling/runners`.
3. Ler o log do scheduler em `logs/integration/scheduler`.
4. Ler o status JSON e o QA CSV em `logs/integration/status`.
5. Se o problema for dashboard, verificar `out/dashboard_financeiro_v1`.

## Regras operacionais
- Nao editar wrappers sem confirmar o runner canonico.
- Nao usar `automation/scripts` ou `automation/scheduler` como fonte de verdade para novos ajustes.
- Registrar qualquer mudanca de job em governanca e inventario.

## Pendencia conhecida
O job de dashboard ainda nao publica status na trilha padrao de logs. Essa convergencia deve entrar na proxima sprint.
