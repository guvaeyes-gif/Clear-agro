SELECT
  opening_balance::numeric(14,2) AS valor
FROM public.vw_finance_cash_projection_30d
WHERE company = 'ALL'
  AND ref_date = current_date
LIMIT 1;
