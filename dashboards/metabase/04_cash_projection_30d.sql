SELECT
  company AS empresa,
  ref_date AS data_referencia,
  opening_balance AS saldo_inicial,
  inflow_amount AS entradas,
  outflow_amount AS saidas,
  net_amount AS saldo_liquido_dia,
  cumulative_net_30d AS acumulado_liquido_30d,
  projected_balance_30d AS saldo_projetado
FROM public.vw_finance_cash_projection_30d
WHERE company = {{company}}
ORDER BY ref_date;

