-- Clear OS / Supabase
-- Migration: 20260311191500_crm_analytics_views.sql
-- Scope: CRM analytical views for app and Metabase consumption.

create or replace view public.vw_sales_targets_summary
with (security_invoker = true)
as
select
  st.id as sales_target_id,
  st.target_year,
  st.period_type,
  st.month_num,
  st.quarter_num,
  case
    when st.period_type = 'month'::sales_target_period then st.target_year::text || '-' || lpad(st.month_num::text, 2, '0')
    else st.target_year::text || '-Q' || st.quarter_num::text
  end as period_key,
  case
    when st.period_type = 'month'::sales_target_period then
      to_char(make_date(st.target_year, st.month_num, 1), 'Mon/YYYY')
    else
      'Q' || st.quarter_num::text || '/' || st.target_year::text
  end as period_label,
  st.state,
  st.sales_rep_id,
  coalesce(st.sales_rep_code, sr.rep_code) as sales_rep_code,
  au.full_name as sales_rep_name,
  st.channel,
  st.crop,
  st.status,
  st.target_value,
  coalesce(st.actual_value, 0) as actual_value,
  st.target_value - coalesce(st.actual_value, 0) as gap_value,
  case
    when st.target_value > 0 then round((coalesce(st.actual_value, 0) / st.target_value) * 100, 2)
    else 0
  end as attainment_pct,
  st.target_volume,
  st.actual_volume,
  st.source_system,
  st.external_ref,
  st.updated_at
from public.sales_targets st
left join public.sales_reps sr on sr.id = st.sales_rep_id
left join public.app_users au on au.id = sr.user_id;

create or replace view public.vw_sales_pipeline_summary
with (security_invoker = true)
as
select
  coalesce(sr.id, so.owner_sales_rep_id) as sales_rep_id,
  coalesce(sr.rep_code, 'SEM_REP') as sales_rep_code,
  coalesce(au.full_name, 'SEM_REP') as sales_rep_name,
  c.state as customer_state,
  so.stage,
  date_trunc('month', so.expected_close_date::timestamp)::date as expected_close_month,
  count(*) as opportunities_count,
  sum(so.expected_value) as pipeline_value,
  sum(so.expected_value * (so.probability::numeric / 100)) as weighted_pipeline_value,
  avg(so.probability)::numeric(10,2) as avg_probability,
  count(*) filter (where ca.next_due_at is null) as opportunities_without_next_step,
  count(*) filter (where ca.next_due_at is not null and ca.next_due_at < now()) as opportunities_with_overdue_step,
  max(so.updated_at) as last_opportunity_update
from public.sales_opportunities so
left join public.customers c on c.id = so.customer_id
left join public.sales_reps sr on sr.id = so.owner_sales_rep_id
left join public.app_users au on au.id = sr.user_id
left join (
  select
    opportunity_id,
    min(due_at) filter (where status = 'open'::crm_activity_status) as next_due_at
  from public.crm_activities
  group by opportunity_id
) ca on ca.opportunity_id = so.id
group by
  coalesce(sr.id, so.owner_sales_rep_id),
  coalesce(sr.rep_code, 'SEM_REP'),
  coalesce(au.full_name, 'SEM_REP'),
  c.state,
  so.stage,
  date_trunc('month', so.expected_close_date::timestamp)::date;

create or replace view public.vw_crm_agent_priority_queue
with (security_invoker = true)
as
select
  'finding'::text as queue_source,
  f.id as queue_item_id,
  f.agent_run_id,
  ar.agent_name,
  f.status::text as status,
  f.severity::text as severity,
  f.title,
  f.description,
  f.customer_id,
  c.legal_name as customer_name,
  f.opportunity_id,
  so.title as opportunity_title,
  f.sales_rep_id,
  coalesce(sr.rep_code, 'SEM_REP') as sales_rep_code,
  coalesce(au.full_name, 'SEM_REP') as sales_rep_name,
  f.due_at,
  null::timestamptz as completed_at,
  case f.severity
    when 'critical'::crm_finding_severity then 100
    when 'high'::crm_finding_severity then 80
    when 'medium'::crm_finding_severity then 60
    else 40
  end as priority_score,
  f.created_at,
  f.updated_at,
  f.recommendation as payload
from public.crm_agent_findings f
left join public.crm_agent_runs ar on ar.id = f.agent_run_id
left join public.customers c on c.id = f.customer_id
left join public.sales_opportunities so on so.id = f.opportunity_id
left join public.sales_reps sr on sr.id = f.sales_rep_id
left join public.app_users au on au.id = sr.user_id
where f.status in ('open'::crm_finding_status, 'acknowledged'::crm_finding_status, 'in_review'::crm_finding_status)

union all

select
  'activity'::text as queue_source,
  a.id as queue_item_id,
  null::uuid as agent_run_id,
  null::text as agent_name,
  a.status::text as status,
  case
    when a.priority = 'critical'::crm_activity_priority then 'critical'
    when a.priority = 'high'::crm_activity_priority then 'high'
    when a.priority = 'normal'::crm_activity_priority then 'medium'
    else 'low'
  end as severity,
  a.subject as title,
  a.notes as description,
  a.customer_id,
  c.legal_name as customer_name,
  a.opportunity_id,
  so.title as opportunity_title,
  a.owner_sales_rep_id as sales_rep_id,
  coalesce(sr.rep_code, 'SEM_REP') as sales_rep_code,
  coalesce(au.full_name, 'SEM_REP') as sales_rep_name,
  a.due_at,
  a.completed_at,
  case
    when a.status = 'overdue'::crm_activity_status then 95
    when a.priority = 'critical'::crm_activity_priority then 90
    when a.priority = 'high'::crm_activity_priority then 75
    when a.priority = 'normal'::crm_activity_priority then 55
    else 35
  end as priority_score,
  a.created_at,
  a.updated_at,
  a.metadata as payload
from public.crm_activities a
left join public.customers c on c.id = a.customer_id
left join public.sales_opportunities so on so.id = a.opportunity_id
left join public.sales_reps sr on sr.id = a.owner_sales_rep_id
left join public.app_users au on au.id = sr.user_id
where a.status in ('open'::crm_activity_status, 'overdue'::crm_activity_status);

