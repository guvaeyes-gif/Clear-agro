WITH latest AS (
  SELECT MAX(snapshot_date) AS snapshot_date
  FROM public.vw_finance_kpis_daily
)
SELECT
  k.ar_open_amount::numeric(14,2) AS valor
FROM public.vw_finance_kpis_daily k
JOIN latest l
  ON l.snapshot_date = k.snapshot_date
WHERE k.company = 'ALL';

