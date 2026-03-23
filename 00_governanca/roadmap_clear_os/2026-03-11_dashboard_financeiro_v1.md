# 2026-03-11 - Dashboard Financeiro v1 (Metabase)

## Entregas
- Migration SQL com views analiticas para consumo no Metabase.
- Definicao de 8 KPIs essenciais do Financeiro.
- Pacote de queries SQL para cards do dashboard.
- Runbook operacional com gate de qualidade (PASS/FAIL).

## Views criadas
- `public.vw_finance_bling_ap_base`
- `public.vw_finance_bling_ar_base`
- `public.vw_finance_data_quality_banner`
- `public.vw_finance_cash_projection_30d`
- `public.vw_finance_ap_aging`
- `public.vw_finance_ar_aging`
- `public.vw_finance_kpis_daily`

## Resultado esperado
- Painel Financeiro v1 com filtros por `company` (`CZ`, `CR`, `ALL`).
- Publicacao com seguranca operacional via gate de qualidade.
- Base pronta para expansao por setor (Comercial, Operacoes, Diretoria).

## Proximo passo
- Publicar cards no Metabase e validar com time financeiro.
- Adicionar notificador automatico quando `quality_status = 'FAIL'`.

