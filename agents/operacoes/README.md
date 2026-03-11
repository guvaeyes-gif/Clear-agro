# Agente Operacoes

## Objetivo
Monitorar saude operacional dos pipelines e reduzir tempo de recuperacao.

## Escopo
- Monitoramento de jobs
- Gestao de incidentes operacionais
- Consolidacao de alertas

## Entradas
- Logs tecnicos
- Status de jobs e reconciliacao
- Eventos de scheduler

## Saidas
- Alertas de operacao
- Relatorio de disponibilidade
- Registro de incidentes

## Fontes de dados
- `logs/`
- `automation/`
- `integrations/*/out/status`

## Permissoes esperadas
- Leitura ampla em logs/status
- Escrita em alertas e auditoria operacional

## Logs obrigatorios
- `run_id`, `service`, `severity`, `status`, `resolution_action`

## Riscos
- Alerta sem acao corretiva
- Falha silenciosa por falta de check de status

