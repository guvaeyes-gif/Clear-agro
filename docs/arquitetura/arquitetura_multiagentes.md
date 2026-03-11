# Arquitetura Multiagentes - Clear OS

## Visao geral
O modelo multiagentes do Clear OS separa responsabilidades por dominio para garantir escalabilidade, isolamento de risco e rastreabilidade.

Agentes:
- Orquestrador
- Financeiro
- CRM
- Compras
- Operacoes

## Divisao de responsabilidades
- Orquestrador: coordena sequencia de jobs, locks e janelas.
- Financeiro: AP/AR, reconciliacao, views de KPI financeiro.
- CRM: entidades comerciais, pipeline de vendas e relacionamento.
- Compras: solicitacoes, fornecedores e controle de compras.
- Operacoes: monitoramento de execucao, alertas e incidentes.

## Permissoes de leitura e escrita
- Orquestrador:
  - Leitura: status de todos os modulos.
  - Escrita: status consolidado, disparo de jobs.
- Financeiro:
  - Leitura: integracoes financeiras e dados mestres.
  - Escrita: tabelas financeiras e artefatos de QA financeiro.
- CRM:
  - Leitura: dados CRM e referencias compartilhadas.
  - Escrita: dados CRM e relatorios do dominio.
- Compras:
  - Leitura: fornecedores, requisicoes e categorias financeiras.
  - Escrita: artefatos do dominio de compras.
- Operacoes:
  - Leitura: logs e estado de jobs.
  - Escrita: alertas operacionais e auditoria tecnica.

## Regras operacionais obrigatorias
- Todo job deve gerar log com `run_id`.
- Toda escrita critica deve ser idempotente.
- Jobs concorrentes devem usar lock logico (ex.: per company/run).
- Falhas devem ter trilha em `logs/*` + status estruturado.
- Agente sem permissao de escrita deve operar em modo read-only.

## Papel do orquestrador
- Ordenar pipeline e dependencias.
- Impedir corrida entre jobs simultaneos.
- Bloquear publicacao quando gate de qualidade estiver `FAIL`.
- Expor estado final da rodada para consumo de dashboards e operacao.

