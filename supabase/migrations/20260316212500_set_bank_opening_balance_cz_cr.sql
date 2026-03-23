-- Clear OS / Supabase
-- Migration: 20260316212500_set_bank_opening_balance_cz_cr.sql
-- Purpose: set official bank opening balance for CZ and CR.
-- Source: Bling /caixas net balance snapshot (2026-03-16).

INSERT INTO public.finance_cash_opening_balance (company, as_of_date, opening_balance, updated_at)
VALUES
  ('CZ', current_date, 435249.14, now()),
  ('CR', current_date, 9828.72, now())
ON CONFLICT (company) DO UPDATE
SET
  as_of_date = EXCLUDED.as_of_date,
  opening_balance = EXCLUDED.opening_balance,
  updated_at = now();

DO $$ BEGIN
  RAISE NOTICE 'Bank opening balance updated: CZ=435249.14, CR=9828.72 (snapshot 2026-03-16).';
END $$;
