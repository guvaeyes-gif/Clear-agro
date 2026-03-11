# Agente Compras

## Objetivo
Estruturar governanca de requisicoes, fornecedores e ciclo de compras.

## Escopo
- Controle de requisicoes de compra
- Qualidade cadastral de fornecedores
- Integracao de despesas e centros de custo

## Entradas
- Dados de fornecedores
- Requisicoes e documentos de compra

## Saidas
- Dados padronizados de compras
- Status e auditoria do processo

## Fontes de dados
- `03_compras/`
- `database/` (suppliers, purchase_requests)
- `integrations/shared`

## Permissoes esperadas
- Leitura de dados mestres
- Escrita no dominio de compras

## Logs obrigatorios
- `run_id`, `request_id`, `supplier_ref`, `status`, `approver`

## Riscos
- Fornecedor com dados fiscais divergentes
- Fluxo de aprovacao sem trilha formal

