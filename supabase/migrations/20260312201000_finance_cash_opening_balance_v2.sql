-- Clear OS / Supabase
-- Migration: 20260312201000_finance_cash_opening_balance_v2.sql
-- Purpose: add opening cash balance model and improve 30d cash projection view.

CREATE TABLE IF NOT EXISTS public.finance_cash_opening_balance (
  company text PRIMARY KEY,
  as_of_date date NOT NULL DEFAULT current_date,
  opening_balance numeric(14,2) NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT finance_cash_opening_balance_company_chk CHECK (company IN ('CZ', 'CR'))
);

INSERT INTO public.finance_cash_opening_balance (company, as_of_date, opening_balance)
VALUES
  ('CZ', current_date, 0),
  ('CR', current_date, 0)
ON CONFLICT (company) DO NOTHING;

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
  UNION
  SELECT DISTINCT company FROM public.finance_cash_opening_balance
),
opening_company AS (
  SELECT
    c.company,
    COALESCE(
      (
        SELECT b.opening_balance
        FROM public.finance_cash_opening_balance b
        WHERE b.company = c.company
          AND b.as_of_date <= current_date
        ORDER BY b.as_of_date DESC
        LIMIT 1
      ),
      0
    )::numeric(14,2) AS opening_balance
  FROM companies c
),
opening_all AS (
  SELECT 'ALL'::text AS company, COALESCE(SUM(opening_balance), 0)::numeric(14,2) AS opening_balance
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
    ob.opening_balance,
    d.inflow_amount,
    d.outflow_amount,
    (d.inflow_amount - d.outflow_amount)::numeric(14,2) AS net_amount,
    SUM(d.inflow_amount - d.outflow_amount) OVER (
      PARTITION BY d.company
      ORDER BY d.ref_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )::numeric(14,2) AS cumulative_net_30d
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
  RAISE NOTICE 'Finance opening balance model and cash projection v2 ready.';
END $$;
