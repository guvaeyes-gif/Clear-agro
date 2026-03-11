-- Clear OS / Supabase
-- Migration: 20260311_005_bling_financial_transactions_v1.sql
-- Purpose: create/update financial_transactions from Bling AP/AR core tables.

CREATE UNIQUE INDEX IF NOT EXISTS ux_financial_transactions_source_external_ref
ON public.financial_transactions(source_system, external_ref);

-- Outflows from accounts_payable
INSERT INTO public.financial_transactions (
  transaction_date,
  direction,
  amount,
  currency_code,
  category_id,
  payable_id,
  receivable_id,
  source_system,
  external_ref,
  status,
  notes,
  metadata
)
SELECT
  COALESCE(ap.paid_at::date, ap.due_date) AS transaction_date,
  'outflow'::transaction_direction AS direction,
  ap.amount,
  ap.currency_code,
  ap.category_id,
  ap.id AS payable_id,
  NULL::uuid AS receivable_id,
  'bling'::text AS source_system,
  'tx_' || ap.external_ref AS external_ref,
  CASE
    WHEN ap.status::text = 'paid' THEN 'reconciled'::transaction_status
    ELSE 'posted'::transaction_status
  END AS status,
  'Generated from Bling accounts_payable' AS notes,
  jsonb_build_object(
    'origin_table', 'accounts_payable',
    'origin_external_ref', ap.external_ref,
    'origin_status', ap.status::text,
    'generated_by', '20260311_005_bling_financial_transactions_v1'
  ) AS metadata
FROM public.accounts_payable ap
WHERE ap.source_system = 'bling'
  AND ap.external_ref IS NOT NULL
  AND ap.amount > 0
ON CONFLICT (source_system, external_ref) DO UPDATE
SET
  transaction_date = EXCLUDED.transaction_date,
  amount = EXCLUDED.amount,
  category_id = EXCLUDED.category_id,
  payable_id = EXCLUDED.payable_id,
  receivable_id = EXCLUDED.receivable_id,
  status = EXCLUDED.status,
  notes = EXCLUDED.notes,
  metadata = COALESCE(public.financial_transactions.metadata, '{}'::jsonb) || EXCLUDED.metadata,
  updated_at = now();

-- Inflows from accounts_receivable
INSERT INTO public.financial_transactions (
  transaction_date,
  direction,
  amount,
  currency_code,
  category_id,
  payable_id,
  receivable_id,
  source_system,
  external_ref,
  status,
  notes,
  metadata
)
SELECT
  COALESCE(ar.received_at::date, ar.due_date) AS transaction_date,
  'inflow'::transaction_direction AS direction,
  ar.amount,
  ar.currency_code,
  ar.category_id,
  NULL::uuid AS payable_id,
  ar.id AS receivable_id,
  'bling'::text AS source_system,
  'tx_' || ar.external_ref AS external_ref,
  CASE
    WHEN ar.status::text = 'received' THEN 'reconciled'::transaction_status
    ELSE 'posted'::transaction_status
  END AS status,
  'Generated from Bling accounts_receivable' AS notes,
  jsonb_build_object(
    'origin_table', 'accounts_receivable',
    'origin_external_ref', ar.external_ref,
    'origin_status', ar.status::text,
    'generated_by', '20260311_005_bling_financial_transactions_v1'
  ) AS metadata
FROM public.accounts_receivable ar
WHERE ar.source_system = 'bling'
  AND ar.external_ref IS NOT NULL
  AND ar.amount > 0
ON CONFLICT (source_system, external_ref) DO UPDATE
SET
  transaction_date = EXCLUDED.transaction_date,
  amount = EXCLUDED.amount,
  category_id = EXCLUDED.category_id,
  payable_id = EXCLUDED.payable_id,
  receivable_id = EXCLUDED.receivable_id,
  status = EXCLUDED.status,
  notes = EXCLUDED.notes,
  metadata = COALESCE(public.financial_transactions.metadata, '{}'::jsonb) || EXCLUDED.metadata,
  updated_at = now();

DO $$ BEGIN
  RAISE NOTICE 'Bling financial_transactions sync completed.';
END $$;
