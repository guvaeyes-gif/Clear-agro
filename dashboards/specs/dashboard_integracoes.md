# Dashboard de Integracoes

## Objetivo do painel
Monitorar saude, volume e divergencia das integracoes do Clear OS, com foco inicial em Bling x Supabase.

## Publico-alvo
Plataforma, Financeiro e Operacoes.

## Metricas
- execucoes por integracao
- sucesso, parcial e falha por empresa
- registros lidos, gravados e com erro
- checks PASS e FAIL da reconciliacao
- tempo medio por etapa

## Filtros
- integracao
- empresa
- status
- periodo

## Frequencia de atualizacao
15 minutos para operacao; diario para visao executiva.

## Fonte de dados
- `logs/integration/status`
- QA CSV de reconciliacao
- `logs/integration/scheduler`

## Prioridade
Alta
