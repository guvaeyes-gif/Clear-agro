-- Clear OS / Supabase
-- Migration: 20260311183000_crm_phase1b_operational_tables.sql
-- Scope: CRM operational tables for targets, activities, custody and agent outputs.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'sales_target_period') THEN
    CREATE TYPE sales_target_period AS ENUM ('month', 'quarter');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'sales_target_status') THEN
    CREATE TYPE sales_target_status AS ENUM ('active', 'paused', 'disabled', 'transferred');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'crm_activity_status') THEN
    CREATE TYPE crm_activity_status AS ENUM ('open', 'completed', 'cancelled', 'overdue');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'crm_activity_priority') THEN
    CREATE TYPE crm_activity_priority AS ENUM ('low', 'normal', 'high', 'critical');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'crm_agent_run_status') THEN
    CREATE TYPE crm_agent_run_status AS ENUM ('queued', 'running', 'success', 'partial', 'failed', 'cancelled');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'crm_finding_severity') THEN
    CREATE TYPE crm_finding_severity AS ENUM ('low', 'medium', 'high', 'critical');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'crm_finding_status') THEN
    CREATE TYPE crm_finding_status AS ENUM ('open', 'acknowledged', 'in_review', 'approved', 'dismissed', 'resolved');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'crm_asset_type') THEN
    CREATE TYPE crm_asset_type AS ENUM ('account', 'opportunity', 'customer', 'other');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'crm_custody_status') THEN
    CREATE TYPE crm_custody_status AS ENUM ('active', 'in_transfer', 'transferred');
  END IF;
END$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'crm_data_issue_status') THEN
    CREATE TYPE crm_data_issue_status AS ENUM ('open', 'acknowledged', 'in_review', 'resolved', 'dismissed');
  END IF;
END$$;

create table if not exists public.sales_targets (
  id uuid primary key default gen_random_uuid(),
  target_year integer not null,
  period_type sales_target_period not null,
  month_num integer,
  quarter_num integer,
  state text not null,
  sales_rep_id uuid references public.sales_reps(id) on delete set null,
  sales_rep_code text,
  channel text,
  crop text,
  target_value numeric(14,2) not null default 0,
  target_volume numeric(14,2),
  actual_value numeric(14,2),
  actual_volume numeric(14,2),
  status sales_target_status not null default 'active',
  source_system text not null default 'manual',
  external_ref text,
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid references public.app_users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sales_targets_year_chk check (target_year >= 2000),
  constraint sales_targets_target_value_chk check (target_value >= 0),
  constraint sales_targets_target_volume_chk check (target_volume is null or target_volume >= 0),
  constraint sales_targets_actual_value_chk check (actual_value is null or actual_value >= 0),
  constraint sales_targets_actual_volume_chk check (actual_volume is null or actual_volume >= 0),
  constraint sales_targets_period_chk check (
    (period_type = 'month'::sales_target_period and month_num between 1 and 12 and quarter_num is null)
    or
    (period_type = 'quarter'::sales_target_period and quarter_num between 1 and 4 and month_num is null)
  )
);

create table if not exists public.crm_activities (
  id uuid primary key default gen_random_uuid(),
  customer_id uuid references public.customers(id) on delete set null,
  opportunity_id uuid references public.sales_opportunities(id) on delete set null,
  owner_sales_rep_id uuid references public.sales_reps(id) on delete set null,
  activity_type text not null,
  subject text not null,
  due_at timestamptz,
  completed_at timestamptz,
  priority crm_activity_priority not null default 'normal',
  status crm_activity_status not null default 'open',
  outcome text,
  source_system text not null default 'manual',
  external_ref text,
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid references public.app_users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint crm_activities_ref_chk check (customer_id is not null or opportunity_id is not null),
  constraint crm_activities_completed_due_chk check (completed_at is null or due_at is null or completed_at >= due_at)
);

create table if not exists public.sales_rep_asset_custody (
  id uuid primary key default gen_random_uuid(),
  asset_type crm_asset_type not null,
  asset_label text,
  customer_id uuid references public.customers(id) on delete set null,
  opportunity_id uuid references public.sales_opportunities(id) on delete set null,
  external_ref text,
  current_sales_rep_id uuid references public.sales_reps(id) on delete set null,
  current_sales_rep_code text,
  previous_sales_rep_id uuid references public.sales_reps(id) on delete set null,
  previous_sales_rep_code text,
  custody_status crm_custody_status not null default 'active',
  transferred_at timestamptz,
  transfer_reason text,
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_by uuid references public.app_users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint sales_rep_asset_custody_ref_chk check (
    (asset_type = 'customer'::crm_asset_type and customer_id is not null)
    or
    (asset_type = 'opportunity'::crm_asset_type and opportunity_id is not null)
    or
    (asset_type in ('account'::crm_asset_type, 'other'::crm_asset_type) and external_ref is not null)
  )
);

create table if not exists public.crm_agent_runs (
  id uuid primary key default gen_random_uuid(),
  run_id text not null unique,
  agent_name text not null,
  workflow_name text,
  triggered_by text not null default 'manual',
  trigger_source text,
  status crm_agent_run_status not null default 'queued',
  started_at timestamptz,
  finished_at timestamptz,
  rows_read integer not null default 0,
  rows_written integer not null default 0,
  findings_count integer not null default 0,
  error_message text,
  payload jsonb not null default '{}'::jsonb,
  created_by uuid references public.app_users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint crm_agent_runs_rows_read_chk check (rows_read >= 0),
  constraint crm_agent_runs_rows_written_chk check (rows_written >= 0),
  constraint crm_agent_runs_findings_count_chk check (findings_count >= 0),
  constraint crm_agent_runs_finished_started_chk check (finished_at is null or started_at is null or finished_at >= started_at)
);

create table if not exists public.crm_agent_findings (
  id uuid primary key default gen_random_uuid(),
  agent_run_id uuid not null references public.crm_agent_runs(id) on delete cascade,
  finding_type text not null,
  severity crm_finding_severity not null default 'medium',
  title text not null,
  description text,
  status crm_finding_status not null default 'open',
  customer_id uuid references public.customers(id) on delete set null,
  opportunity_id uuid references public.sales_opportunities(id) on delete set null,
  sales_rep_id uuid references public.sales_reps(id) on delete set null,
  related_record_type text,
  related_record_id text,
  recommendation jsonb not null default '{}'::jsonb,
  payload jsonb not null default '{}'::jsonb,
  due_at timestamptz,
  resolved_at timestamptz,
  resolved_by uuid references public.app_users(id) on delete set null,
  created_by uuid references public.app_users(id) on delete set null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint crm_agent_findings_resolved_chk check (resolved_at is null or resolved_at >= created_at)
);

create table if not exists public.crm_data_quality_issues (
  id uuid primary key default gen_random_uuid(),
  detected_run_id uuid references public.crm_agent_runs(id) on delete set null,
  source_system text not null default 'crm',
  entity_name text not null,
  issue_type text not null,
  severity crm_finding_severity not null default 'medium',
  status crm_data_issue_status not null default 'open',
  fingerprint text not null unique,
  customer_id uuid references public.customers(id) on delete set null,
  opportunity_id uuid references public.sales_opportunities(id) on delete set null,
  sales_rep_id uuid references public.sales_reps(id) on delete set null,
  issue_payload jsonb not null default '{}'::jsonb,
  detected_at timestamptz not null default now(),
  resolved_at timestamptz,
  resolved_by uuid references public.app_users(id) on delete set null,
  notes text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint crm_data_quality_issues_resolved_chk check (resolved_at is null or resolved_at >= detected_at)
);

create unique index if not exists uq_sales_targets_grain
on public.sales_targets (
  target_year,
  period_type,
  coalesce(month_num, 0),
  coalesce(quarter_num, 0),
  state,
  coalesce(sales_rep_code, sales_rep_id::text, ''),
  coalesce(channel, ''),
  coalesce(crop, '')
);

create unique index if not exists uq_sales_targets_external_ref
on public.sales_targets(source_system, external_ref)
where external_ref is not null;

create unique index if not exists uq_crm_activities_external_ref
on public.crm_activities(source_system, external_ref)
where external_ref is not null;

create unique index if not exists uq_sales_rep_asset_custody_asset
on public.sales_rep_asset_custody (
  asset_type,
  coalesce(customer_id::text, opportunity_id::text, external_ref, '')
);

create index if not exists idx_sales_targets_sales_rep_id on public.sales_targets(sales_rep_id);
create index if not exists idx_sales_targets_year_period on public.sales_targets(target_year, period_type, month_num, quarter_num);
create index if not exists idx_sales_targets_status on public.sales_targets(status);
create index if not exists idx_crm_activities_owner_due on public.crm_activities(owner_sales_rep_id, due_at);
create index if not exists idx_crm_activities_status on public.crm_activities(status);
create index if not exists idx_crm_activities_customer_id on public.crm_activities(customer_id);
create index if not exists idx_crm_activities_opportunity_id on public.crm_activities(opportunity_id);
create index if not exists idx_sales_rep_asset_custody_current_rep on public.sales_rep_asset_custody(current_sales_rep_id, custody_status);
create index if not exists idx_crm_agent_runs_status on public.crm_agent_runs(status, created_at);
create index if not exists idx_crm_agent_findings_run_id on public.crm_agent_findings(agent_run_id);
create index if not exists idx_crm_agent_findings_status_severity on public.crm_agent_findings(status, severity);
create index if not exists idx_crm_agent_findings_sales_rep_id on public.crm_agent_findings(sales_rep_id);
create index if not exists idx_crm_data_quality_issues_status_severity on public.crm_data_quality_issues(status, severity);
create index if not exists idx_crm_data_quality_issues_sales_rep_id on public.crm_data_quality_issues(sales_rep_id);

DROP TRIGGER IF EXISTS trg_set_updated_at_sales_targets ON public.sales_targets;
CREATE TRIGGER trg_set_updated_at_sales_targets
BEFORE UPDATE ON public.sales_targets
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_set_updated_at_crm_activities ON public.crm_activities;
CREATE TRIGGER trg_set_updated_at_crm_activities
BEFORE UPDATE ON public.crm_activities
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_set_updated_at_sales_rep_asset_custody ON public.sales_rep_asset_custody;
CREATE TRIGGER trg_set_updated_at_sales_rep_asset_custody
BEFORE UPDATE ON public.sales_rep_asset_custody
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_set_updated_at_crm_agent_runs ON public.crm_agent_runs;
CREATE TRIGGER trg_set_updated_at_crm_agent_runs
BEFORE UPDATE ON public.crm_agent_runs
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_set_updated_at_crm_agent_findings ON public.crm_agent_findings;
CREATE TRIGGER trg_set_updated_at_crm_agent_findings
BEFORE UPDATE ON public.crm_agent_findings
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

DROP TRIGGER IF EXISTS trg_set_updated_at_crm_data_quality_issues ON public.crm_data_quality_issues;
CREATE TRIGGER trg_set_updated_at_crm_data_quality_issues
BEFORE UPDATE ON public.crm_data_quality_issues
FOR EACH ROW EXECUTE FUNCTION set_updated_at();
