# Mapa de Integracoes do Clear OS

## Objetivo
Documentar as integracoes existentes, seus pontos de origem e destino, forma de disparo e nivel de criticidade operacional.

## Integracoes ativas ou parcialmente ativas
| Integracao | Origem | Destino | Runner ou script principal | Disparo | Credencial | Criticidade | Observacoes |
|---|---|---|---|---|---|---|---|
| Bling sync ERP | `bling_api/sync_erp.py` no repo legado | caches JSONL do Bling | `integrations/bling/runners/run_bling_supabase_daily.ps1` | automatico diario e manual | arquivo local Bling | alta | roda por empresa `CZ` e `CR` |
| Finance ingest hub Bling | caches JSONL Bling | staging e status em `logs/integration` | skill `finance_ingest_hub.py` acionada pelo runner diario | automatico diario e manual | depende de arquivos locais do cache | alta | config por empresa em `integrations/bling/config` |
| Geracao de migration Bling -> Supabase | caches JSONL Bling | `supabase/migrations` e status JSON | `integrations/bling/load/generate_bling_supabase_import.py` | automatico diario e manual | nao usa segredo proprio, depende do cache | alta | gera migration incremental e status |
| Push para Supabase | migrations locais | banco Supabase remoto | `npx supabase db push` via runner diario | automatico diario e manual | token Supabase | alta | altera ambiente ligado ao projeto remoto |
| Reconciliacao Bling x Supabase | caches Bling e tabelas AP/AR no Supabase | status JSON e QA CSV | `integrations/bling/reconciliation/reconcile_bling_supabase.py` | automatico diario e manual | token Supabase e service role via CLI | alta | principal gate tecnico do fluxo financeiro |
| Publicacao do dashboard financeiro | specs, SQL e quality gate | `out/dashboard_financeiro_v1` | `automation/jobs/run_finance_dashboard_publisher.cmd` | manual | opcional Telegram | media | artefato tecnico de BI, nao altera integracao de negocio |

## Integracoes mapeadas para futuro proximo
| Integracao | Estado | Dependencia |
|---|---|---|
| CRM -> Supabase | planejada ou parcial | contratos em `integrations/crm` e `agents/crm` |
| Sheets -> Clear OS | estrutural | padronizacao em `integrations/sheets` |
| Observabilidade -> Dashboards tecnicos | especificacao criada nesta rodada | trilha padrao de logs e status |

## Fonte, destino e dependencias por fluxo Bling
| Etapa | Fonte | Destino | Artefato principal |
|---|---|---|---|
| Sync | API Bling | cache JSONL local | `contas_pagar_cache*.jsonl`, `contas_receber_cache*.jsonl`, `contatos_cache*.jsonl` |
| Ingest | cache JSONL | staging e status | `logs/integration/staging`, `logs/integration/status` |
| Load preparation | cache JSONL | migration SQL | `supabase/migrations/*_bling_import_v1.sql` |
| Push | migration SQL | Supabase | `supabase db push --linked` |
| Reconciliacao | Bling + Supabase | status e QA | `bling_supabase_reconciliation_*_status.json`, `*_qa.csv` |

## Scripts que dependem de credenciais
- `integrations/bling/runners/run_bling_supabase_daily.ps1`
- `integrations/bling/reconciliation/reconcile_bling_supabase.py`
- `automation/jobs/run_finance_dashboard_publisher.cmd` apenas para Telegram, quando habilitado

## Scripts que dependem de arquivos locais
- Configs de ingestao por empresa em `integrations/bling/config`
- Cache JSONL do repo legado `bling_api`
- Token Supabase em arquivo local do usuario
- Arquivo de credencial Bling em caminho legado de governanca

## Scripts criticos para producao
- `integrations/bling/runners/run_bling_supabase_daily.ps1`
- `integrations/bling/runners/register_bling_supabase_daily_task.ps1`
- `integrations/bling/load/generate_bling_supabase_import.py`
- `integrations/bling/reconciliation/reconcile_bling_supabase.py`
- `automation/jobs/run_bling_supabase_daily_cz.cmd`
- `automation/jobs/run_bling_supabase_daily_cr.cmd`

## Fluxos que exigem lock
- Runner diario Bling por empresa: lock por `company_code` e janela de execucao.
- `supabase db push`: lock global por ambiente.
- Reconciliacao por empresa: lock por `company_code`.
- Mudanca em scheduler: lock administrativo, nunca concorrente.

## Fluxos que precisam ser idempotentes
- Geracao de migration incremental.
- Upsert de dados Bling em tabelas financeiras.
- Reconciliacao com status e QA por execucao.
- Publicacao de status final do dashboard.

## Diretriz canonica
- A fonte canonica dos fluxos Bling esta em `integrations/bling`.
- `automation/jobs` deve apenas disparar os runners canonicos.
- Qualquer referencia a `11_agentes_automacoes/.../pipeline` deve ser tratada como legado ou dependencia transitoria.
