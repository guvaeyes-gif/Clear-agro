# Finance Recon Hub

Conciliacao bancaria multi-bancos com Bling (contas a receber/pagar), com relatorio Excel e alerta Telegram.

## Escopo MVP

- Entrada multi-bancos via `CSV` e `OFX` em `data/bank/inbox`
- Saida:
  - `out/conciliacao_YYYYMMDD_HHMMSS.xlsx`
  - `out/conciliacao_matches_*.csv`
  - `out/conciliacao_nao_conciliado_*.csv`
- Integracao Bling por cache local:
  - `contas_receber_cache.jsonl`
  - `contas_pagar_cache.jsonl`

## Como rodar

```powershell
cd C:\Users\cesar.zarovski\projects\finance-recon-hub
pip install -r requirements.txt
python scripts\run_reconciliation.py
```

### Execucao completa (pre-sync + conciliacao com fallback)

```powershell
python scripts\run_daily_pipeline.py
```

### Job E2E: Fechamento financeiro com validacao

```powershell
powershell -ExecutionPolicy Bypass -File scripts\jobs\run_financial_close_job.ps1 -AllowStaleData
```

### AIOS local fallback: monthly-fin-close

Executa o orquestrador de fechamento financeiro via estrutura AIOS local (`agents/`, `workflows/`, `policies/`, `connectors/`).

```powershell
python scripts\run_monthly_fin_close.py --run-id 20260224_230000
```

Arquivos gerados:

- `out/aios/monthly-fin-close/<run_id>/execution.json`
- `out/aios/monthly-fin-close/<run_id>/orchestrator_status.json`
- `out/aios/monthly-fin-close/<run_id>/orchestrator_summary.md`
- `out/aios/monthly-fin-close/<run_id>/pm_governance_decision.json`
- `out/aios/monthly-fin-close/latest.json`

## Configuracao (opcional)

Variaveis de ambiente:

- `RECON_BANK_INBOX_DIR` (default: `data/bank/inbox`)
- `RECON_OUTPUT_DIR` (default: `out`)
- `BLING_API_DIR` (default auto-detect: `C:\Users\cesar.zarovski\CRM_Clear_Agro\bling_api`)
- `RECON_DATE_WINDOW_DAYS` (default: `2`)
- `RECON_AMOUNT_TOLERANCE` (default: `1.00`)
- `TELEGRAM_BOT_TOKEN` e `TELEGRAM_CHAT_ID` (alerta resumo)

## Agendamento diario (Windows)

```powershell
powershell -ExecutionPolicy Bypass -File scripts\register_daily_task.ps1 -StartTime "06:20"
```

Tarefa criada (default): `Finance-Control-Tower-Daily`

## Logs operacionais

- `out/recon_task.log`
- `out/run_logs/recon_runs.jsonl`
- `out/run_logs/daily_pipeline.jsonl`

## Runbook

- `docs/CONCILIACAO_BANCARIA_RUNBOOK.md`
- `docs/OPERATING_FLOWS.md`
- `docs/SYNKRA_AIOS_INTEGRATION.md`
- `docs/ADOPTION_PLAN_2_PHASES.md`
- `AGENTS.md`
- `DEFINITION_OF_DONE.md`
- `DATA_GOVERNANCE.md`
