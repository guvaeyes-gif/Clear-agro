SELECT
  ref_date AS data_referencia,
  inflow_amount::numeric(14,2) AS entradas,
  outflow_amount::numeric(14,2) AS saidas,
  projected_balance_30d::numeric(14,2) AS saldo_projetado
FROM public.vw_finance_cash_projection_30d
WHERE company = 'ALL'
ORDER BY ref_date;

