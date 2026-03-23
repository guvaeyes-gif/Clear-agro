# 12_integracoes_agent - Escopo

## Dentro do escopo
- Monitorar integracoes ativas
- Tratar falhas de webhook
- Padronizar contratos de API
- Documentar credenciais e seguranca

## Fora do escopo
- Integrar sistema sem documentacao minima
- Expor token em texto aberto

## Escalonamento
- Escalar bloqueios tecnicos para 10_data_agent ou 12_integracoes_agent.
- Escalar conflito de prioridade para 00_orquestrador_clear.
- Escalar decisoes criticas para aprovador humano da area.
