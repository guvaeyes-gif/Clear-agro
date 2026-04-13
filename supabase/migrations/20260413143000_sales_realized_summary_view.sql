create or replace view public.vw_sales_realized_summary
with (security_invoker = true)
as
select
  ar.id as receivable_id,
  ar.issue_date as transaction_date,
  ar.due_date,
  ar.invoice_number,
  ar.amount as revenue_amount,
  ar.status::text as receivable_status,
  ar.source_system,
  ar.external_ref,
  ar.metadata,
  c.id as customer_id,
  c.legal_name as customer_name,
  c.trade_name as customer_trade_name,
  c.state as customer_state,
  c.country as customer_country,
  coalesce(ar.metadata ->> 'company', c.metadata ->> 'company', '') as company,
  coalesce(sr.id, c.owner_sales_rep_id) as sales_rep_id,
  coalesce(sr.rep_code, 'SEM_REP') as sales_rep_code,
  coalesce(au.full_name, 'SEM_REP') as sales_rep_name
from public.accounts_receivable ar
left join public.customers c on c.id = ar.customer_id
left join public.sales_reps sr on sr.id = c.owner_sales_rep_id
left join public.app_users au on au.id = sr.user_id
where ar.source_system = 'bling';
