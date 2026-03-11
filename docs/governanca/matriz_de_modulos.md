# Matriz de Modulos

| Modulo | Leitura principal | Escrita principal | Owner |
|---|---|---|---|
| database | schema, migrations | views, migrations, qa | Dados/Engenharia |
| integrations/bling | caches Bling, configs | carga Supabase, status e qa | Integracoes |
| dashboards | views analiticas | cards/metadados de painel | BI/Financeiro |
| agents/financeiro | AP/AR, transacoes, QA | artefatos de analise financeira | Financeiro |
| agents/crm | dados comerciais | artefatos CRM | Comercial |
| agents/compras | fornecedores e requisicoes | artefatos compras | Compras |
| agents/operacoes | logs e status | alertas operacionais | Operacoes |
| agents/orchestrator | estado de todos modulos | disparo e consolidacao de jobs | Plataforma |
| security | politicas e acessos | auditoria de seguranca | Governanca |

## Limites
- modulo nao deve escrever fora do seu dominio sem contrato claro.
- alteracoes cross-modulo exigem PR com impacto explicito.

