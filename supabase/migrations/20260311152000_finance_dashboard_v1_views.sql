-- Clear OS / Supabase
-- Migration: 20260311152000_finance_dashboard_v1_views.sql
-- Purpose: finance dashboard v1 analytical views (Metabase-ready).

CREATE OR REPLACE VIEW public.vw_finance_bling_ap_base AS
SELECT
  ap.id,
  ap.external_ref,
  ap.issue_date,
  ap.due_date,
  ap.amount,
  ap.currency_code,
  ap.status::text AS status,
  COALESCE(NULLIF(ap.metadata ->> 'company', ''), 'UNKNOWN') AS company,
  ap.metadata,
  ap.created_at,
  ap.updated_at
FROM public.accounts_payable ap
WHERE ap.source_system = 'bling'
  AND ap.external_ref IS NOT NULL;

CREATE OR REPLACE VIEW public.vw_finance_bling_ar_base AS
SELECT
  ar.id,
  ar.external_ref,
  ar.issue_date,
  ar.due_date,
  ar.amount,
  ar.currency_code,
  ar.status::text AS status,
  COALESCE(NULLIF(ar.metadata ->> 'company', ''), 'UNKNOWN') AS company,
  ar.metadata,
  ar.created_at,
  ar.updated_at
FROM public.accounts_receivable ar
WHERE ar.source_system = 'bling'
  AND ar.external_ref IS NOT NULL;

CREATE OR REPLACE VIEW public.vw_finance_data_quality_banner AS
WITH ap AS (
  SELECT company, updated_at
  FROM public.vw_finance_bling_ap_base
),
ar AS (
  SELECT company, updated_at
  FROM public.vw_finance_bling_ar_base
),
agg AS (
  SELECT
    COALESCE((SELECT MAX(updated_at) FROM ap), to_timestamp(0)) AS ap_updated_at,
    COALESCE((SELECT MAX(updated_at) FROM ar), to_timestamp(0)) AS ar_updated_at,
    COALESCE((SELECT COUNT(*) FROM ap WHERE company = 'UNKNOWN'), 0) AS unknown_ap_rows,
    COALESCE((SELECT COUNT(*) FROM ar WHERE company = 'UNKNOWN'), 0) AS unknown_ar_rows,
    EXISTS (SELECT 1 FROM ap WHERE company = 'CZ') AS has_ap_cz,
    EXISTS (SELECT 1 FROM ap WHERE company = 'CR') AS has_ap_cr,
    EXISTS (SELECT 1 FROM ar WHERE company = 'CZ') AS has_ar_cz,
    EXISTS (SELECT 1 FROM ar WHERE company = 'CR') AS has_ar_cr
)
SELECT
  CASE
    WHEN (has_ap_cz AND has_ar_cz AND has_ap_cr AND has_ar_cr)
      AND unknown_ap_rows = 0
      AND unknown_ar_rows = 0
      AND GREATEST(ap_updated_at, ar_updated_at) >= now() - interval '36 hours'
    THEN 'PASS'
    ELSE 'FAIL'
  END AS quality_status,
  GREATEST(ap_updated_at, ar_updated_at) AS latest_updated_at,
  unknown_ap_rows,
  unknown_ar_rows,
  has_ap_cz,
  has_ap_cr,
  has_ar_cz,
  has_ar_cr,
  CASE
    WHEN (has_ap_cz AND has_ar_cz AND has_ap_cr AND has_ar_cr)
      AND unknown_ap_rows = 0
      AND unknown_ar_rows = 0
      AND GREATEST(ap_updated_at, ar_updated_at) >= now() - interval '36 hours'
    THEN 'Data quality gate passed.'
    ELSE 'Data quality gate failed: verify reconciliation/status artifacts.'
  END AS message
FROM agg;

CREATE OR REPLACE VIEW public.vw_finance_cash_projection_30d AS
WITH ap AS (
  SELECT company, due_date, amount
  FROM public.vw_finance_bling_ap_base
  WHERE status IN ('draft', 'open', 'approved', 'overdue')
),
ar AS (
  SELECT company, due_date, amount
  FROM public.vw_finance_bling_ar_base
  WHERE status IN ('open', 'partially_paid', 'overdue')
),
days AS (
  SELECT generate_series(current_date, current_date + interval '29 day', interval '1 day')::date AS ref_date
),
companies AS (
  SELECT DISTINCT company FROM ap
  UNION
  SELECT DISTINCT company FROM ar
),
events_company AS (
  SELECT company, due_date AS ref_date, SUM(amount)::numeric(14,2) AS inflow_amount, 0::numeric(14,2) AS outflow_amount
  FROM ar
  WHERE due_date BETWEEN current_date AND current_date + interval '29 day'
  GROUP BY company, due_date
  UNION ALL
  SELECT company, due_date AS ref_date, 0::numeric(14,2) AS inflow_amount, SUM(amount)::numeric(14,2) AS outflow_amount
  FROM ap
  WHERE due_date BETWEEN current_date AND current_date + interval '29 day'
  GROUP BY company, due_date
),
events_all AS (
  SELECT 'ALL'::text AS company, ref_date, SUM(inflow_amount)::numeric(14,2) AS inflow_amount, SUM(outflow_amount)::numeric(14,2) AS outflow_amount
  FROM events_company
  GROUP BY ref_date
),
events AS (
  SELECT * FROM events_company
  UNION ALL
  SELECT * FROM events_all
),
calendar AS (
  SELECT c.company, d.ref_date
  FROM companies c
  CROSS JOIN days d
  UNION ALL
  SELECT 'ALL'::text, d.ref_date
  FROM days d
),
daily AS (
  SELECT
    cal.company,
    cal.ref_date,
    COALESCE(SUM(e.inflow_amount), 0)::numeric(14,2) AS inflow_amount,
    COALESCE(SUM(e.outflow_amount), 0)::numeric(14,2) AS outflow_amount
  FROM calendar cal
  LEFT JOIN events e
    ON e.company = cal.company
   AND e.ref_date = cal.ref_date
  GROUP BY cal.company, cal.ref_date
)
SELECT
  company,
  ref_date,
  inflow_amount,
  outflow_amount,
  (inflow_amount - outflow_amount)::numeric(14,2) AS net_amount,
  SUM(inflow_amount - outflow_amount) OVER (
    PARTITION BY company
    ORDER BY ref_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )::numeric(14,2) AS cumulative_net_30d
FROM daily;

CREATE OR REPLACE VIEW public.vw_finance_ap_aging AS
WITH base AS (
  SELECT company, due_date, amount
  FROM public.vw_finance_bling_ap_base
  WHERE status IN ('draft', 'open', 'approved', 'overdue')
),
classified AS (
  SELECT
    company,
    amount,
    CASE
      WHEN due_date < current_date - interval '90 day' THEN '>90'
      WHEN due_date < current_date - interval '60 day' THEN '61-90'
      WHEN due_date < current_date - interval '30 day' THEN '31-60'
      WHEN due_date < current_date THEN '0-30'
      WHEN due_date <= current_date + interval '30 day' THEN 'a_vencer_30'
      ELSE 'a_vencer_mais_30'
    END AS aging_bucket
  FROM base
),
totals AS (
  SELECT company, SUM(amount)::numeric(14,2) AS total_amount
  FROM base
  GROUP BY company
),
company_rows AS (
  SELECT
    c.company,
    c.aging_bucket,
    COUNT(*)::bigint AS titles_count,
    SUM(c.amount)::numeric(14,2) AS amount_total,
    CASE WHEN t.total_amount > 0 THEN ROUND((SUM(c.amount) / t.total_amount) * 100, 2) ELSE 0 END AS amount_pct
  FROM classified c
  JOIN totals t ON t.company = c.company
  GROUP BY c.company, c.aging_bucket, t.total_amount
),
all_rows AS (
  SELECT
    'ALL'::text AS company,
    aging_bucket,
    COUNT(*)::bigint AS titles_count,
    SUM(amount)::numeric(14,2) AS amount_total,
    CASE WHEN (SELECT SUM(amount) FROM base) > 0
      THEN ROUND((SUM(amount) / (SELECT SUM(amount) FROM base)) * 100, 2)
      ELSE 0
    END AS amount_pct
  FROM classified
  GROUP BY aging_bucket
)
SELECT * FROM company_rows
UNION ALL
SELECT * FROM all_rows;

CREATE OR REPLACE VIEW public.vw_finance_ar_aging AS
WITH base AS (
  SELECT company, due_date, amount
  FROM public.vw_finance_bling_ar_base
  WHERE status IN ('open', 'partially_paid', 'overdue')
),
classified AS (
  SELECT
    company,
    amount,
    CASE
      WHEN due_date < current_date - interval '90 day' THEN '>90'
      WHEN due_date < current_date - interval '60 day' THEN '61-90'
      WHEN due_date < current_date - interval '30 day' THEN '31-60'
      WHEN due_date < current_date THEN '0-30'
      WHEN due_date <= current_date + interval '30 day' THEN 'a_vencer_30'
      ELSE 'a_vencer_mais_30'
    END AS aging_bucket
  FROM base
),
totals AS (
  SELECT company, SUM(amount)::numeric(14,2) AS total_amount
  FROM base
  GROUP BY company
),
company_rows AS (
  SELECT
    c.company,
    c.aging_bucket,
    COUNT(*)::bigint AS titles_count,
    SUM(c.amount)::numeric(14,2) AS amount_total,
    CASE WHEN t.total_amount > 0 THEN ROUND((SUM(c.amount) / t.total_amount) * 100, 2) ELSE 0 END AS amount_pct
  FROM classified c
  JOIN totals t ON t.company = c.company
  GROUP BY c.company, c.aging_bucket, t.total_amount
),
all_rows AS (
  SELECT
    'ALL'::text AS company,
    aging_bucket,
    COUNT(*)::bigint AS titles_count,
    SUM(amount)::numeric(14,2) AS amount_total,
    CASE WHEN (SELECT SUM(amount) FROM base) > 0
      THEN ROUND((SUM(amount) / (SELECT SUM(amount) FROM base)) * 100, 2)
      ELSE 0
    END AS amount_pct
  FROM classified
  GROUP BY aging_bucket
)
SELECT * FROM company_rows
UNION ALL
SELECT * FROM all_rows;

CREATE OR REPLACE VIEW public.vw_finance_kpis_daily AS
WITH ap_open AS (
  SELECT company, SUM(amount)::numeric(14,2) AS amount
  FROM public.vw_finance_bling_ap_base
  WHERE status IN ('draft', 'open', 'approved', 'overdue')
  GROUP BY company
),
ar_open AS (
  SELECT company, SUM(amount)::numeric(14,2) AS amount
  FROM public.vw_finance_bling_ar_base
  WHERE status IN ('open', 'partially_paid', 'overdue')
  GROUP BY company
),
ap_overdue AS (
  SELECT company, SUM(amount)::numeric(14,2) AS amount
  FROM public.vw_finance_bling_ap_base
  WHERE status IN ('draft', 'open', 'approved', 'overdue')
    AND due_date < current_date
  GROUP BY company
),
ar_overdue AS (
  SELECT company, SUM(amount)::numeric(14,2) AS amount
  FROM public.vw_finance_bling_ar_base
  WHERE status IN ('open', 'partially_paid', 'overdue')
    AND due_date < current_date
  GROUP BY company
),
companies AS (
  SELECT DISTINCT company FROM public.vw_finance_bling_ap_base
  UNION
  SELECT DISTINCT company FROM public.vw_finance_bling_ar_base
),
projection AS (
  SELECT company, SUM(net_amount)::numeric(14,2) AS net_projection_30d
  FROM public.vw_finance_cash_projection_30d
  GROUP BY company
),
company_rows AS (
  SELECT
    c.company,
    COALESCE(apo.amount, 0)::numeric(14,2) AS ap_open_amount,
    COALESCE(aro.amount, 0)::numeric(14,2) AS ar_open_amount,
    COALESCE(apd.amount, 0)::numeric(14,2) AS ap_overdue_amount,
    COALESCE(ard.amount, 0)::numeric(14,2) AS ar_overdue_amount,
    COALESCE(p.net_projection_30d, 0)::numeric(14,2) AS net_projection_30d
  FROM companies c
  LEFT JOIN ap_open apo ON apo.company = c.company
  LEFT JOIN ar_open aro ON aro.company = c.company
  LEFT JOIN ap_overdue apd ON apd.company = c.company
  LEFT JOIN ar_overdue ard ON ard.company = c.company
  LEFT JOIN projection p ON p.company = c.company
),
all_row AS (
  SELECT
    'ALL'::text AS company,
    SUM(ap_open_amount)::numeric(14,2) AS ap_open_amount,
    SUM(ar_open_amount)::numeric(14,2) AS ar_open_amount,
    SUM(ap_overdue_amount)::numeric(14,2) AS ap_overdue_amount,
    SUM(ar_overdue_amount)::numeric(14,2) AS ar_overdue_amount,
    SUM(net_projection_30d)::numeric(14,2) AS net_projection_30d
  FROM company_rows
),
base AS (
  SELECT * FROM company_rows
  UNION ALL
  SELECT * FROM all_row
)
SELECT
  current_date AS snapshot_date,
  b.company,
  b.ap_open_amount,
  b.ar_open_amount,
  b.ap_overdue_amount,
  b.ar_overdue_amount,
  b.net_projection_30d,
  dq.quality_status AS reconciliation_status,
  dq.latest_updated_at AS reconciliation_reference_at
FROM base b
CROSS JOIN public.vw_finance_data_quality_banner dq;

DO $$ BEGIN
  RAISE NOTICE 'Finance dashboard v1 views ready.';
END $$;

