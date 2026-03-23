# 13_monitor_operacao - Escopo

## Dentro do escopo
- Centralizar status dos jobs
- Emitir alertas criticos
- Acompanhar falhas recorrentes
- Manter rotina de monitoramento diario

## Fora do escopo
- Silenciar alerta critico sem registro
- Reprocessar lote sem trilha de auditoria

## Escalonamento
- Escalar bloqueios tecnicos para 10_data_agent ou 12_integracoes_agent.
- Escalar conflito de prioridade para 00_orquestrador_clear.
- Escalar decisoes criticas para aprovador humano da area.
