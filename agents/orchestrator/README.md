# Agente Orchestrator

## Objetivo
Coordenar execucoes entre modulos e garantir ordem, lock e idempotencia.

## Escopo
- Disparo de jobs
- Controle de dependencias
- Consolidacao de status

## Entradas
- Status de integracoes
- Sinais de scheduler
- Regras operacionais

## Saidas
- Status consolidado
- Alertas de falha
- Trilha de auditoria de execucao

## Fontes de dados
- `automation/`
- `logs/`
- `integrations/*/out/status`

## Permissoes esperadas
- Leitura global
- Escrita em logs/status consolidados
- Sem alteracao direta de dados de negocio

## Logs obrigatorios
- `run_id`, `job_name`, `stage`, `status`, `started_at`, `finished_at`

## Riscos
- Execucao concorrente sem lock
- Encadeamento incorreto entre jobs

