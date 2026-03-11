# Agente CRM

## Objetivo
Organizar dados comerciais e suporte analitico para funil, clientes e vendas.

## Escopo
- Ingestao/normalizacao de dados comerciais
- Curadoria de entidades CRM
- Suporte a indicadores de vendas

## Entradas
- Dados de clientes e oportunidades
- Integracoes CRM/sheets

## Saidas
- Modelos de dados CRM prontos para consumo
- Status de qualidade de dados comerciais

## Fontes de dados
- `integrations/crm`
- `integrations/sheets`
- tabelas CRM no banco

## Permissoes esperadas
- Leitura nas fontes comerciais
- Escrita em dominio CRM e artefatos de QA CRM

## Logs obrigatorios
- `run_id`, `source`, `entity`, `status`, `rows_affected`

## Riscos
- Duplicidade de cliente
- Chave de contato inconsistente entre fontes

