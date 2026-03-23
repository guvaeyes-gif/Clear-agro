# Dashboard Financeiro v1 (Metabase)

## Objetivo
Entregar o painel financeiro inicial do Clear OS com 8 KPIs essenciais:

1. AP aberto (R$)
2. AR aberto (R$)
3. AP vencido (R$)
4. AR vencido (R$)
5. Fluxo liquido previsto 30d (R$)
6. Aging AP (R$ e %)
7. Aging AR (R$ e %)
8. Status de qualidade/reconciliacao (PASS/FAIL)

## Fonte de dados
Views SQL no Supabase:

- `public.vw_finance_kpis_daily`
- `public.vw_finance_ap_aging`
- `public.vw_finance_ar_aging`
- `public.vw_finance_cash_projection_30d`
- `public.vw_finance_data_quality_banner`

## Publicacao no Metabase
Colecao recomendada:

- `Clear OS / Financeiro / v1`

Cards recomendados (usar SQL em `metabase/sql_cards`):

- `01_kpis_diarios.sql`
- `02_ap_aging.sql`
- `03_ar_aging.sql`
- `04_cash_projection_30d.sql`
- `05_data_quality_banner.sql`

Filtros globais:

- `company` (`CZ`, `CR`, `ALL`)
- periodo (default: ultimos 30 dias)

## Rotina operacional
- Jobs de dados:
  - `ClearOS-Bling-Supabase-Daily-CZ` (06:10)
  - `ClearOS-Bling-Supabase-Daily-CR` (06:25)
- Atualizacao de painel recomendada: 06:40 (America/Sao_Paulo)

## Gate de qualidade
- Se `vw_finance_data_quality_banner.quality_status = 'FAIL'`, exibir banner:
  - `Dados em validacao. Verifique reconciliacao antes de tomar decisao.`

