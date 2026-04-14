# CRM Supabase Model - Phase 1B

## Objetivo
Fechar o modelo de dados CRM no Supabase para substituir as dependencias locais do app atual (`xlsx`, `jsonl` e `metas.db`) por tabelas transacionais e operacionais com RLS.

## Tabelas adicionadas
- `sales_targets`
  - substitui o nucleo da tabela local `metas`
  - cobre metas mensais/trimestrais por UF, vendedor, canal e cultura
- `crm_activities`
  - cobre follow-ups, proximo passo, disciplina comercial e carteira parada
- `sales_rep_asset_custody`
  - substitui a logica local de `ativos_custodia`
  - registra transferencia de carteira, conta, cliente ou oportunidade
- `crm_agent_runs`
  - trilha de execucao dos agentes CRM
- `crm_agent_findings`
  - achados e recomendacoes produzidos pelos agentes
- `crm_data_quality_issues`
  - backlog estruturado de problemas de qualidade de dados comerciais

## Mapeamento do modelo local atual
- `metas.db.metas` -> `public.sales_targets`
- `metas.db.ativos_custodia` -> `public.sales_rep_asset_custody`
- `metas.db.audit_log` -> fase posterior via `event_history` ou trilha transacional por servico
- `base_unificada.xlsx:oportunidades` -> `public.sales_opportunities`
- `base_unificada.xlsx:clientes` -> `public.customers` (quando a curadoria for concluida)
- `data_proximo_passo` / follow-up comercial -> `public.crm_activities`

## Racional de modelagem
- `sales_targets` foi desenhada para manter compatibilidade com o app atual, inclusive nos filtros por `ano`, `periodo`, `UF` e `vendedor`.
- `sales_rep_asset_custody` permite registrar transferencias sem depender imediatamente de todas as referencias estarem em UUID no app.
- `crm_agent_runs` e `crm_agent_findings` separam claramente execucao de agente e efeito analitico, evitando escrita direta no core transacional.
- `crm_data_quality_issues` cria uma fila rastreavel para duplicidade, ownership inconsistente e falta de proximo passo.

## Migrations relacionadas
- `supabase/migrations/20260311183000_crm_phase1b_operational_tables.sql`
- `supabase/migrations/20260311184500_crm_phase1b_rls.sql`

## Proximo passo tecnico recomendado
1. Criar views CRM consumiveis pelo app (`vw_sales_targets_summary`, `vw_sales_pipeline_summary`, `vw_crm_agent_priority_queue`).
2. Substituir `src/metas_db.py` por um repositorio Supabase/Postgres.
3. Migrar a pagina `Metas Comerciais` do app para ler e gravar em `sales_targets`.
4. Migrar a aba de `Transferencia` para `sales_rep_asset_custody`.

## Fonte compartilhada de metas
- Fonte recomendada: Google Sheets compartilhada com a aba `metas`.
- Fluxo operacional:
  1. editar a planilha compartilhada;
  2. clicar no botao `Validar planilha compartilhada` no dashboard;
  3. clicar em `Sincronizar agora`.
- O importador normaliza colunas, grava as metas mensais e cria automaticamente o fechamento trimestral somando os 3 meses.
