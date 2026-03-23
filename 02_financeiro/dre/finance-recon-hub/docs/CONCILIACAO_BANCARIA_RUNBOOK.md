# Runbook: Conciliação Bancária + Bling

## Objetivo
Operar a conciliação automática diária com fallback quando o pre-sync do Bling falhar.

## Janela operacional
- 06:20: tarefa `Finance-Control-Tower-Daily`
- Fluxo interno:
  1. Pre-sync Bling (`contas_receber`, `contas_pagar`)
  2. Conciliação bancária
  3. Relatórios + log + alerta Telegram

## Entradas
- Pasta de extratos: `data/bank/inbox`
- Formatos suportados: `.csv`, `.ofx`
- Bling cache: `C:\Users\cesar.zarovski\CRM_Clear_Agro\bling_api\contas_*_cache.jsonl`

## Saídas esperadas
- `out/conciliacao_<run_id>.xlsx`
- `out/conciliacao_matches_<run_id>.csv`
- `out/conciliacao_nao_conciliado_<run_id>.csv`
- `out/conciliacao_pendente_bling_<run_id>.csv`
- Logs:
  - `out/run_logs/recon_runs.jsonl`
  - `out/run_logs/daily_pipeline.jsonl`

## Fallback (modo degradado)
Se o pre-sync falhar:
- a conciliação continua com dados existentes do cache Bling,
- a execução é marcada como `data_stale=true` nos logs.

## Checklist diário (5 min)
1. Confirmar que há extratos novos em `data/bank/inbox`.
2. Verificar último `run_id` em `out/run_logs/daily_pipeline.jsonl`.
3. Abrir `conciliacao_<run_id>.xlsx` e revisar:
   - `Resumo`
   - `Causas_Nao_Conciliado`
4. Tratar pendências prioritárias:
   - `AMOUNT_MISMATCH`
   - `DATE_OUT_OF_WINDOW`
   - `DESCRIPTION_DIVERGENCE`

## Incidentes comuns
- `NO_BLING_TITLES`: cache Bling vazio ou desatualizado.
- sem arquivo de saída: verificar `recon_task.log` e `daily_pipeline.jsonl`.
- Telegram não envia: validar `TELEGRAM_BOT_TOKEN` e `TELEGRAM_CHAT_ID`.
