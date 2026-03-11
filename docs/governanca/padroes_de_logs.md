# Padroes de Logs

## Objetivo
Padronizar observabilidade e auditoria de execucao.

## Campos obrigatorios por execucao
- `run_id`
- `job_name`
- `module`
- `company` (quando aplicavel)
- `started_at`
- `finished_at`
- `status` (`success|partial|failed`)
- `error_message` (quando falha)

## Estrutura recomendada
- Logs tecnicos: `logs/integration`, `logs/agents`, `logs/audit`
- Status estruturado: JSON em `out/status` por pipeline
- QA: CSV com checks e PASS/FAIL

## Convencao de nome
- Logs: `<job>_<run_id>.log`
- Status: `<modulo>_<run_id>_status.json`
- QA: `<modulo>_<run_id>_qa.csv`

