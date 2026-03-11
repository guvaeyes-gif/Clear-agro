SELECT
  snapshot_date,
  company,
  ap_open_amount,
  ar_open_amount,
  ap_overdue_amount,
  ar_overdue_amount,
  net_projection_30d,
  reconciliation_status,
  reconciliation_reference_at
FROM public.vw_finance_kpis_daily
WHERE company = {{company}}
ORDER BY snapshot_date DESC;

