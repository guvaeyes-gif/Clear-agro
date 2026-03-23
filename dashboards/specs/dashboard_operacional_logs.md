# Dashboard Operacional de Logs

## Objetivo do painel
Dar visibilidade rapida sobre execucoes, falhas e atrasos dos jobs tecnicos do Clear OS.

## Publico-alvo
Plataforma, Operacoes e lideres tecnicos.

## Metricas
- ultima execucao por job
- status final por job
- duracao por execucao
- quantidade de erros por dia
- jobs sem log nas ultimas 24h

## Filtros
- job
- modulo
- empresa
- status
- periodo

## Frequencia de atualizacao
15 minutos ou leitura sob demanda.

## Fonte de dados
- `logs/integration/runs`
- `logs/integration/status`
- `logs/agents`

## Prioridade
Alta
