SELECT
  company,
  ref_date,
  inflow_amount,
  outflow_amount,
  net_amount,
  cumulative_net_30d
FROM public.vw_finance_cash_projection_30d
WHERE company = {{company}}
ORDER BY ref_date;

