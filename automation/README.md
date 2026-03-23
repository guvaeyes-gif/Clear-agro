# Automation

Estrutura para operacao programada:
- `jobs/`: comandos de execucao
- `scheduler/`: registro e gestao de agendamentos
- `scripts/`: camada de compatibilidade e utilitarios de automacao

## Regra atual
- Fonte canonica dos runners Bling: `integrations/bling/runners/`
- `automation/jobs/` deve apenas disparar runners canonicos
- `automation/scripts/` e `automation/scheduler/` nao devem carregar logica propria de pipeline; servem como compatibilidade transitora

