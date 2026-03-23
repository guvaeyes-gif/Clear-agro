-- Clear OS / Supabase
-- Policy blueprint: 20260310_001_phase1a_rls_blueprint.sql
-- Note: apply after 20260310_001_phase1a_core_tables.sql and seed at least one super_admin user.

create or replace function public.current_app_user_id()
returns uuid
language sql
stable
as $$
  select au.id
  from public.app_users au
  where au.auth_user_id = auth.uid()
  limit 1;
$$;

create or replace function public.current_user_role()
returns app_role
language sql
stable
as $$
  select au.role
  from public.app_users au
  where au.auth_user_id = auth.uid()
  limit 1;
$$;

create or replace function public.has_any_role(roles app_role[])
returns boolean
language sql
stable
as $$
  select coalesce(public.current_user_role() = any(roles), false);
$$;

alter table public.app_users enable row level security;
alter table public.sales_reps enable row level security;
alter table public.customers enable row level security;
alter table public.financial_categories enable row level security;
alter table public.suppliers enable row level security;
alter table public.sales_opportunities enable row level security;
alter table public.purchase_requests enable row level security;
alter table public.accounts_payable enable row level security;
alter table public.accounts_receivable enable row level security;
alter table public.financial_transactions enable row level security;

create policy app_users_select_self_or_admin
on public.app_users
for select
using (
  id = public.current_app_user_id()
  or public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'executive_admin'::app_role])
);

create policy app_users_update_admin_only
on public.app_users
for update
using (public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role]))
with check (public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role]));

create policy customers_select_sales_fin_exec_admin
on public.customers
for select
using (
  public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'executive_admin'::app_role, 'sales_team'::app_role, 'finance_team'::app_role])
);

create policy sales_opportunities_select_sales_scope
on public.sales_opportunities
for select
using (
  public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'executive_admin'::app_role])
  or (
    public.current_user_role() = 'sales_team'::app_role
    and (
      created_by = public.current_app_user_id()
      or owner_sales_rep_id in (
        select sr.id from public.sales_reps sr where sr.user_id = public.current_app_user_id()
      )
    )
  )
);

create policy accounts_payable_select_finance_admin
on public.accounts_payable
for select
using (
  public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'executive_admin'::app_role, 'finance_team'::app_role])
);

create policy accounts_receivable_select_finance_sales_admin
on public.accounts_receivable
for select
using (
  public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'executive_admin'::app_role, 'finance_team'::app_role, 'sales_team'::app_role])
);

create policy financial_transactions_select_finance_admin
on public.financial_transactions
for select
using (
  public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'executive_admin'::app_role, 'finance_team'::app_role])
);

create policy suppliers_select_proc_fin_admin
on public.suppliers
for select
using (
  public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'executive_admin'::app_role, 'procurement_team'::app_role, 'finance_team'::app_role])
);

create policy purchase_requests_select_proc_owner_admin
on public.purchase_requests
for select
using (
  public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'executive_admin'::app_role, 'procurement_team'::app_role])
  or requester_user_id = public.current_app_user_id()
);

create policy finance_write_payable
on public.accounts_payable
for all
using (public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'finance_team'::app_role]))
with check (public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'finance_team'::app_role]));

create policy finance_write_receivable
on public.accounts_receivable
for all
using (public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'finance_team'::app_role]))
with check (public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'finance_team'::app_role]));

create policy finance_write_transactions
on public.financial_transactions
for all
using (public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'finance_team'::app_role]))
with check (public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'finance_team'::app_role]));

create policy sales_write_opportunities
on public.sales_opportunities
for all
using (
  public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role])
  or (
    public.current_user_role() = 'sales_team'::app_role
    and created_by = public.current_app_user_id()
  )
)
with check (
  public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role])
  or (
    public.current_user_role() = 'sales_team'::app_role
    and created_by = public.current_app_user_id()
  )
);

create policy procurement_write_requests
on public.purchase_requests
for all
using (
  public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'procurement_team'::app_role])
)
with check (
  public.has_any_role(array['super_admin'::app_role, 'system_admin'::app_role, 'procurement_team'::app_role])
);
