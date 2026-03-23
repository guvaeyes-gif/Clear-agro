SELECT
  projected_balance_30d::numeric(14,2) AS valor
FROM public.vw_finance_cash_projection_30d
WHERE company = 'ALL'
ORDER BY ref_date DESC
LIMIT 1;

