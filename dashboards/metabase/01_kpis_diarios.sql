SELECT
  snapshot_date AS data_referencia,
  company AS empresa,
  ap_open_amount AS ap_em_aberto,
  ar_open_amount AS ar_em_aberto,
  ap_overdue_amount AS ap_vencido,
  ar_overdue_amount AS ar_vencido,
  net_projection_30d AS projecao_liquida_30d,
  reconciliation_status AS status_conciliacao,
  reconciliation_reference_at AS referencia_conciliacao
FROM public.vw_finance_kpis_daily
WHERE company = {{company}}
ORDER BY snapshot_date DESC;

