# Checagem Diaria do Clear OS

## Objetivo
Executar uma checagem rapida da saude operacional antes do inicio do trabalho diario.

## Checklist
1. Confirmar se as tasks `ClearOS-Bling-Supabase-Daily-CZ` e `ClearOS-Bling-Supabase-Daily-CR` executaram na janela esperada.
2. Verificar `logs/integration/scheduler/task_runner_cz.log` e `task_runner_cr.log`.
3. Conferir se existe status JSON recente em `logs/integration/status`.
4. Conferir se a reconciliacao mais recente terminou com `status = success` e `fail = 0`.
5. Verificar se houve migration nova inesperada em `supabase/migrations`.
6. Se houver publicacao de dashboard planejada, conferir `out/dashboard_financeiro_v1/dashboard_healthcheck.json`.
7. Registrar qualquer anomalia em `logs/audit` ou no canal operacional adotado.

## Sinais de alerta
- Falta de status para `CZ` ou `CR`.
- Wrapper com erro repetido no scheduler.
- QA CSV com `FAIL`.
- Dashboard healthcheck com `ready = false`.
- Duracao muito acima do padrao historico.
