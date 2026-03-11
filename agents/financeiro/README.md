# Agente Financeiro

## Objetivo
Operar AP/AR, reconciliacao e indicadores financeiros com rastreabilidade.

## Escopo
- Ingestao financeira
- Carga para Supabase
- Reconciliacao Bling x Supabase
- Publicacao de KPIs financeiros

## Entradas
- Caches Bling
- Migrations e views financeiras
- Configs de integracao

## Saidas
- Status e QA financeiros
- Dados atualizados em tabelas/APIs financeiras
- Artefatos para dashboard financeiro

## Fontes de dados
- `integrations/bling/*`
- `database/migrations`
- `database/views`

## Permissoes esperadas
- Leitura em configuracoes e dados fonte
- Escrita em tabelas financeiras e status/qa

## Logs obrigatorios
- `run_id`, `company`, `records_read`, `records_written`, `status`, `error_message`

## Riscos
- Divergencia de empresa (`CZ`/`CR`)
- Conflitos de chave unica em upsert

