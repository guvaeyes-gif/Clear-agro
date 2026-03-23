-- Clear OS / Supabase
-- Migration: 20260314103500_finance_cash_opening_from_ap_ar.sql
-- Purpose: derive opening cash balance directly from Bling AP/AR base tables
--          to keep CZ/CR mapping consistent and avoid stale intermediary totals.

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
ap_opening AS (
  SELECT
    COALESCE(NULLIF(ap2.metadata ->> 'company', ''), NULLIF(ap2.metadata ->> 'empresa', ''), 'UNKNOWN') AS company,
    COALESCE(ap2.paid_at::date, ap2.due_date) AS transaction_date,
    ap2.amount::numeric(14,2) AS amount
  FROM public.accounts_payable ap2
  WHERE ap2.source_system = 'bling'
    AND ap2.amount > 0
),
ar_opening AS (
  SELECT
    COALESCE(NULLIF(ar2.metadata ->> 'company', ''), NULLIF(ar2.metadata ->> 'empresa', ''), 'UNKNOWN') AS company,
    COALESCE(ar2.received_at::date, ar2.due_date) AS transaction_date,
    ar2.amount::numeric(14,2) AS amount
  FROM public.accounts_receivable ar2
  WHERE ar2.source_system = 'bling'
    AND ar2.amount > 0
),
days AS (
  SELECT generate_series(current_date, current_date + interval '29 day', interval '1 day')::date AS ref_date
),
companies AS (
  SELECT DISTINCT company FROM ap
  UNION
  SELECT DISTINCT company FROM ar
  UNION
  SELECT DISTINCT company FROM public.finance_cash_opening_balance
  UNION
  SELECT DISTINCT company FROM ap_opening
  UNION
  SELECT DISTINCT company FROM ar_opening
),
opening_from_bling AS (
  SELECT
    company,
    SUM(net_amount)::numeric(14,2) AS opening_balance
  FROM (
    SELECT company, amount::numeric(14,2) AS net_amount
    FROM ar_opening
    WHERE transaction_date < current_date

    UNION ALL

    SELECT company, (-amount)::numeric(14,2) AS net_amount
    FROM ap_opening
    WHERE transaction_date < current_date
  ) src
  GROUP BY company
),
opening_from_bling_all AS (
  SELECT
    COALESCE(SUM(net_amount), 0)::numeric(14,2) AS opening_balance
  FROM (
    SELECT amount::numeric(14,2) AS net_amount
    FROM ar_opening
    WHERE transaction_date < current_date

    UNION ALL

    SELECT (-amount)::numeric(14,2) AS net_amount
    FROM ap_opening
    WHERE transaction_date < current_date
  ) src
),
manual_latest AS (
  SELECT DISTINCT ON (company)
    company,
    opening_balance
  FROM public.finance_cash_opening_balance
  WHERE as_of_date <= current_date
  ORDER BY company, as_of_date DESC
),
opening_company AS (
  SELECT
    c.company,
    CASE
      WHEN ml.opening_balance IS NOT NULL AND ml.opening_balance <> 0 THEN ml.opening_balance
      ELSE COALESCE(ofb.opening_balance, 0)
    END::numeric(14,2) AS opening_balance
  FROM companies c
  LEFT JOIN opening_from_bling ofb
    ON ofb.company = c.company
  LEFT JOIN manual_latest ml
    ON ml.company = c.company
),
opening_all AS (
  SELECT
    'ALL'::text AS company,
    CASE
      WHEN COUNT(*) FILTER (WHERE opening_balance <> 0) > 0
        THEN COALESCE(SUM(opening_balance), 0)
      ELSE (SELECT opening_balance FROM opening_from_bling_all)
    END::numeric(14,2) AS opening_balance
  FROM opening_company
),
opening_base AS (
  SELECT * FROM opening_company
  UNION ALL
  SELECT * FROM opening_all
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
),
base AS (
  SELECT
    d.company,
    d.ref_date,
    d.inflow_amount,
    d.outflow_amount,
    (d.inflow_amount - d.outflow_amount)::numeric(14,2) AS net_amount,
    SUM(d.inflow_amount - d.outflow_amount) OVER (
      PARTITION BY d.company
      ORDER BY d.ref_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )::numeric(14,2) AS cumulative_net_30d,
    ob.opening_balance
  FROM daily d
  JOIN opening_base ob
    ON ob.company = d.company
)
SELECT
  company,
  ref_date,
  inflow_amount,
  outflow_amount,
  net_amount,
  cumulative_net_30d,
  opening_balance,
  (opening_balance + cumulative_net_30d)::numeric(14,2) AS projected_balance_30d
FROM base;

DO $$ BEGIN
  RAISE NOTICE 'Cash projection opening balance now derives from Bling AP/AR.';
END $$;
