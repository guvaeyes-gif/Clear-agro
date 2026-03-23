# Verificacao de Logs

## Objetivo
Padronizar a leitura dos logs tecnicos e status operacionais.

## Onde olhar primeiro
- `logs/integration/scheduler`
- `logs/integration/runs`
- `logs/integration/status`
- `logs/agents`
- `logs/audit`

## Sequencia recomendada
1. Comecar pelo log do scheduler.
2. Identificar o runner chamado.
3. Abrir o log tecnico da execucao.
4. Confirmar o status JSON final.
5. Se houver reconciliacao, abrir o QA CSV.
6. Registrar incidente ou excecao relevante em auditoria.

## O que um log valido deve ter
- carimbo de tempo
- inicio e fim de etapa
- contexto minimo de empresa e job
- mensagem de erro pesquisavel em caso de falha

## O que um status valido deve ter
- `execution_id` ou `run_id`
- `job_name`
- `module_name`
- `status`
- inicio e fim
- contagens principais
- referencia para payload ou QA quando aplicavel
