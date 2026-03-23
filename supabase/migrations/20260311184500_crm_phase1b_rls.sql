-- Clear OS / Supabase
-- Migration: 20260311184500_crm_phase1b_rls.sql
-- Scope: Baseline RLS for CRM operational phase 1B tables.

alter table public.sales_targets enable row level security;
alter table public.crm_activities enable row level security;
alter table public.sales_rep_asset_custody enable row level security;
alter table public.crm_agent_runs enable row level security;
alter table public.crm_agent_findings enable row level security;
alter table public.crm_data_quality_issues enable row level security;

DROP POLICY IF EXISTS sales_targets_select_sales_fin_exec_admin ON public.sales_targets;
CREATE POLICY sales_targets_select_sales_fin_exec_admin
ON public.sales_targets
FOR SELECT
USING (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role,
    'executive_admin'::app_role,
    'sales_team'::app_role,
    'finance_team'::app_role
  ])
);

DROP POLICY IF EXISTS sales_targets_write_exec_admin_only ON public.sales_targets;
CREATE POLICY sales_targets_write_exec_admin_only
ON public.sales_targets
FOR ALL
USING (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role,
    'executive_admin'::app_role
  ])
)
WITH CHECK (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role,
    'executive_admin'::app_role
  ])
);

DROP POLICY IF EXISTS crm_activities_select_sales_scope ON public.crm_activities;
CREATE POLICY crm_activities_select_sales_scope
ON public.crm_activities
FOR SELECT
USING (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role,
    'executive_admin'::app_role
  ])
  OR (
    public.current_user_role() = 'sales_team'::app_role
    AND (
      created_by = public.current_app_user_id()
      OR owner_sales_rep_id IN (
        SELECT sr.id FROM public.sales_reps sr WHERE sr.user_id = public.current_app_user_id()
      )
      OR customer_id IN (
        SELECT c.id
        FROM public.customers c
        JOIN public.sales_reps sr ON sr.id = c.owner_sales_rep_id
        WHERE sr.user_id = public.current_app_user_id()
      )
    )
  )
);

DROP POLICY IF EXISTS crm_activities_write_sales_scope ON public.crm_activities;
CREATE POLICY crm_activities_write_sales_scope
ON public.crm_activities
FOR ALL
USING (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role,
    'executive_admin'::app_role
  ])
  OR (
    public.current_user_role() = 'sales_team'::app_role
    AND created_by = public.current_app_user_id()
  )
)
WITH CHECK (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role,
    'executive_admin'::app_role
  ])
  OR (
    public.current_user_role() = 'sales_team'::app_role
    AND created_by = public.current_app_user_id()
  )
);

DROP POLICY IF EXISTS sales_rep_asset_custody_select_sales_scope ON public.sales_rep_asset_custody;
CREATE POLICY sales_rep_asset_custody_select_sales_scope
ON public.sales_rep_asset_custody
FOR SELECT
USING (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role,
    'executive_admin'::app_role
  ])
  OR (
    public.current_user_role() = 'sales_team'::app_role
    AND (
      current_sales_rep_id IN (
        SELECT sr.id FROM public.sales_reps sr WHERE sr.user_id = public.current_app_user_id()
      )
      OR previous_sales_rep_id IN (
        SELECT sr.id FROM public.sales_reps sr WHERE sr.user_id = public.current_app_user_id()
      )
    )
  )
);

DROP POLICY IF EXISTS sales_rep_asset_custody_write_exec_admin_only ON public.sales_rep_asset_custody;
CREATE POLICY sales_rep_asset_custody_write_exec_admin_only
ON public.sales_rep_asset_custody
FOR ALL
USING (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role,
    'executive_admin'::app_role
  ])
)
WITH CHECK (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role,
    'executive_admin'::app_role
  ])
);

DROP POLICY IF EXISTS crm_agent_runs_select_scope ON public.crm_agent_runs;
CREATE POLICY crm_agent_runs_select_scope
ON public.crm_agent_runs
FOR SELECT
USING (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role,
    'executive_admin'::app_role
  ])
  OR (
    public.current_user_role() = 'sales_team'::app_role
    AND created_by = public.current_app_user_id()
  )
);

DROP POLICY IF EXISTS crm_agent_runs_write_admin_only ON public.crm_agent_runs;
CREATE POLICY crm_agent_runs_write_admin_only
ON public.crm_agent_runs
FOR ALL
USING (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role
  ])
)
WITH CHECK (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role
  ])
);

DROP POLICY IF EXISTS crm_agent_findings_select_sales_scope ON public.crm_agent_findings;
CREATE POLICY crm_agent_findings_select_sales_scope
ON public.crm_agent_findings
FOR SELECT
USING (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role,
    'executive_admin'::app_role
  ])
  OR (
    public.current_user_role() = 'sales_team'::app_role
    AND (
      created_by = public.current_app_user_id()
      OR sales_rep_id IN (
        SELECT sr.id FROM public.sales_reps sr WHERE sr.user_id = public.current_app_user_id()
      )
      OR customer_id IN (
        SELECT c.id
        FROM public.customers c
        JOIN public.sales_reps sr ON sr.id = c.owner_sales_rep_id
        WHERE sr.user_id = public.current_app_user_id()
      )
    )
  )
);

DROP POLICY IF EXISTS crm_agent_findings_write_admin_only ON public.crm_agent_findings;
CREATE POLICY crm_agent_findings_write_admin_only
ON public.crm_agent_findings
FOR ALL
USING (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role
  ])
)
WITH CHECK (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role
  ])
);

DROP POLICY IF EXISTS crm_data_quality_issues_select_sales_scope ON public.crm_data_quality_issues;
CREATE POLICY crm_data_quality_issues_select_sales_scope
ON public.crm_data_quality_issues
FOR SELECT
USING (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role,
    'executive_admin'::app_role
  ])
  OR (
    public.current_user_role() = 'sales_team'::app_role
    AND (
      sales_rep_id IN (
        SELECT sr.id FROM public.sales_reps sr WHERE sr.user_id = public.current_app_user_id()
      )
      OR customer_id IN (
        SELECT c.id
        FROM public.customers c
        JOIN public.sales_reps sr ON sr.id = c.owner_sales_rep_id
        WHERE sr.user_id = public.current_app_user_id()
      )
    )
  )
);

DROP POLICY IF EXISTS crm_data_quality_issues_write_admin_only ON public.crm_data_quality_issues;
CREATE POLICY crm_data_quality_issues_write_admin_only
ON public.crm_data_quality_issues
FOR ALL
USING (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role
  ])
)
WITH CHECK (
  public.has_any_role(array[
    'super_admin'::app_role,
    'system_admin'::app_role
  ])
);
