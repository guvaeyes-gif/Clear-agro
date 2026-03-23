-- Clear OS / Supabase
-- Migration: 20260310_001_phase1a_core_tables.sql
-- Scope: Phase 1A core tables (10 first tables)

create extension if not exists pgcrypto;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'app_role') THEN
    CREATE TYPE app_role AS ENUM (
      'super_admin',
      'executive_admin',
      'finance_team',
      'sales_team',
      'procurement_team',
      'production_team',
      'quality_team',
      'rnd_team',
      'logistics_team',
      'hr_team',
      'system_admin'
    );
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'record_status') THEN
    CREATE TYPE record_status AS ENUM ('active', 'inactive', 'archived');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'opportunity_stage') THEN
    CREATE TYPE opportunity_stage AS ENUM ('lead', 'qualified', 'proposal', 'negotiation', 'won', 'lost');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'payable_status') THEN
    CREATE TYPE payable_status AS ENUM ('draft', 'open', 'approved', 'paid', 'overdue', 'cancelled');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'receivable_status') THEN
    CREATE TYPE receivable_status AS ENUM ('open', 'partially_paid', 'received', 'overdue', 'cancelled');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'transaction_direction') THEN
    CREATE TYPE transaction_direction AS ENUM ('inflow', 'outflow');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'transaction_status') THEN
    CREATE TYPE transaction_status AS ENUM ('posted', 'reconciled', 'voided');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'purchase_request_status') THEN
    CREATE TYPE purchase_request_status AS ENUM ('draft', 'requested', 'approved', 'quoted', 'ordered', 'cancelled');
  END IF;
END$$;

create or replace function set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table if not exists public.app_users (
  id uuid primary key default gen_random_uuid(),
  auth_user_id uuid unique references auth.users(id) on delete set null,
  full_name text not null,
  email text not null unique,
  role app_role not null,
  department text,
  is_active boolean not null default true,
  status record_status not null default 'active',
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.sales_reps (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null unique references public.app_users(id) on delete cascade,
  rep_code text unique,
  region text,
  commission_rate numeric(5,2) not null default 0,
  status record_status not null default 'active',
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid references public.app_users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.customers (
  id uuid primary key default gen_random_uuid(),
  legal_name text not null,
  trade_name text,
  tax_id text unique,
  segment text,
  city text,
  state text,
  country text,
  owner_sales_rep_id uuid references public.sales_reps(id) on delete set null,
  risk_level text not null default 'medium',
  status record_status not null default 'active',
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid references public.app_users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists public.financial_categories (
  id uuid primary key default gen_random_uuid(),
  code text not null unique,
  name text not null,
  category_group text not null,
  is_cash_flow boolean not null default true,
  status record_status not null default 'active',
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid references public.app_users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint financial_categories_group_chk
    check (category_group in ('revenue', 'expense', 'asset', 'liability', 'equity', 'tax', 'transfer'))
);

create table if not exists public.suppliers (
  id uuid primary key default gen_random_uuid(),
  legal_name text not null,
  trade_name text,
  tax_id text unique,
  email text,
  phone text,
  city text,
  state text,
  country text,
  payment_terms_days integer not null default 30,
  status record_status not null default 'active',
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid references public.app_users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint suppliers_payment_terms_chk check (payment_terms_days >= 0)
);

create table if not exists public.sales_opportunities (
  id uuid primary key default gen_random_uuid(),
  customer_id uuid not null references public.customers(id) on delete restrict,
  owner_sales_rep_id uuid references public.sales_reps(id) on delete set null,
  title text not null,
  stage opportunity_stage not null default 'lead',
  expected_value numeric(14,2) not null default 0,
  probability smallint not null default 0,
  expected_close_date date,
  source text,
  status record_status not null default 'active',
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid references public.app_users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sales_opportunities_expected_value_chk check (expected_value >= 0),
  constraint sales_opportunities_probability_chk check (probability between 0 and 100)
);

create table if not exists public.purchase_requests (
  id uuid primary key default gen_random_uuid(),
  requester_user_id uuid not null references public.app_users(id) on delete restrict,
  supplier_id uuid references public.suppliers(id) on delete set null,
  title text not null,
  description text,
  needed_by date not null,
  estimated_amount numeric(14,2) not null,
  currency_code char(3) not null default 'BRL',
  status purchase_request_status not null default 'requested',
  priority text not null default 'normal',
  approved_by uuid references public.app_users(id) on delete set null,
  approved_at timestamptz,
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint purchase_requests_estimated_amount_chk check (estimated_amount >= 0),
  constraint purchase_requests_priority_chk check (priority in ('low', 'normal', 'high', 'critical'))
);

create table if not exists public.accounts_payable (
  id uuid primary key default gen_random_uuid(),
  supplier_id uuid not null references public.suppliers(id) on delete restrict,
  category_id uuid not null references public.financial_categories(id) on delete restrict,
  description text not null,
  document_number text,
  issue_date date not null,
  due_date date not null,
  amount numeric(14,2) not null,
  currency_code char(3) not null default 'BRL',
  status payable_status not null default 'open',
  approved_by uuid references public.app_users(id) on delete set null,
  approved_at timestamptz,
  paid_at timestamptz,
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid references public.app_users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint accounts_payable_amount_chk check (amount >= 0),
  constraint accounts_payable_due_issue_chk check (due_date >= issue_date)
);

create table if not exists public.accounts_receivable (
  id uuid primary key default gen_random_uuid(),
  customer_id uuid not null references public.customers(id) on delete restrict,
  category_id uuid not null references public.financial_categories(id) on delete restrict,
  description text not null,
  invoice_number text,
  issue_date date not null,
  due_date date not null,
  amount numeric(14,2) not null,
  currency_code char(3) not null default 'BRL',
  status receivable_status not null default 'open',
  received_at timestamptz,
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid references public.app_users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint accounts_receivable_amount_chk check (amount >= 0),
  constraint accounts_receivable_due_issue_chk check (due_date >= issue_date)
);

create table if not exists public.financial_transactions (
  id uuid primary key default gen_random_uuid(),
  transaction_date date not null,
  direction transaction_direction not null,
  amount numeric(14,2) not null,
  currency_code char(3) not null default 'BRL',
  category_id uuid not null references public.financial_categories(id) on delete restrict,
  payable_id uuid references public.accounts_payable(id) on delete set null,
  receivable_id uuid references public.accounts_receivable(id) on delete set null,
  source_system text not null default 'manual',
  external_ref text,
  status transaction_status not null default 'posted',
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid references public.app_users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint financial_transactions_amount_chk check (amount > 0),
  constraint financial_transactions_ref_chk check (
    payable_id is not null or receivable_id is not null or source_system = 'manual'
  )
);

create index if not exists idx_app_users_role on public.app_users(role);
create index if not exists idx_customers_owner_sales_rep_id on public.customers(owner_sales_rep_id);
create index if not exists idx_sales_opportunities_stage on public.sales_opportunities(stage);
create index if not exists idx_sales_opportunities_expected_close_date on public.sales_opportunities(expected_close_date);
create index if not exists idx_purchase_requests_status_needed_by on public.purchase_requests(status, needed_by);
create index if not exists idx_accounts_payable_due_date_status on public.accounts_payable(due_date, status);
create index if not exists idx_accounts_receivable_due_date_status on public.accounts_receivable(due_date, status);
create index if not exists idx_financial_transactions_transaction_date on public.financial_transactions(transaction_date);
create index if not exists idx_financial_transactions_category_id on public.financial_transactions(category_id);

DROP TRIGGER IF EXISTS trg_set_updated_at_app_users ON public.app_users;
CREATE TRIGGER trg_set_updated_at_app_users
BEFORE UPDATE ON public.app_users
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_set_updated_at_sales_reps ON public.sales_reps;
CREATE TRIGGER trg_set_updated_at_sales_reps
BEFORE UPDATE ON public.sales_reps
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_set_updated_at_customers ON public.customers;
CREATE TRIGGER trg_set_updated_at_customers
BEFORE UPDATE ON public.customers
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_set_updated_at_financial_categories ON public.financial_categories;
CREATE TRIGGER trg_set_updated_at_financial_categories
BEFORE UPDATE ON public.financial_categories
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_set_updated_at_suppliers ON public.suppliers;
CREATE TRIGGER trg_set_updated_at_suppliers
BEFORE UPDATE ON public.suppliers
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_set_updated_at_sales_opportunities ON public.sales_opportunities;
CREATE TRIGGER trg_set_updated_at_sales_opportunities
BEFORE UPDATE ON public.sales_opportunities
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_set_updated_at_purchase_requests ON public.purchase_requests;
CREATE TRIGGER trg_set_updated_at_purchase_requests
BEFORE UPDATE ON public.purchase_requests
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_set_updated_at_accounts_payable ON public.accounts_payable;
CREATE TRIGGER trg_set_updated_at_accounts_payable
BEFORE UPDATE ON public.accounts_payable
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_set_updated_at_accounts_receivable ON public.accounts_receivable;
CREATE TRIGGER trg_set_updated_at_accounts_receivable
BEFORE UPDATE ON public.accounts_receivable
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_set_updated_at_financial_transactions ON public.financial_transactions;
CREATE TRIGGER trg_set_updated_at_financial_transactions
BEFORE UPDATE ON public.financial_transactions
FOR EACH ROW EXECUTE FUNCTION set_updated_at();
