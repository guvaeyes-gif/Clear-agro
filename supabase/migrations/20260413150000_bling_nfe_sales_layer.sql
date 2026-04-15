create table if not exists public.bling_nfe_documents (
  id uuid primary key default gen_random_uuid(),
  company text not null check (company in ('CZ', 'CR')),
  bling_nfe_id bigint not null,
  invoice_number text,
  issue_datetime timestamp without time zone,
  operation_datetime timestamp without time zone,
  access_key text,
  series text,
  customer_bling_id text,
  customer_name text,
  customer_tax_id text,
  customer_state text,
  natureza_id text,
  salesperson_bling_id text,
  total_amount numeric(14,2) not null default 0,
  freight_amount numeric(14,2) not null default 0,
  first_cfop text,
  payload jsonb not null default '{}'::jsonb,
  source_system text not null default 'bling',
  external_ref text not null,
  created_at timestamp with time zone not null default now(),
  updated_at timestamp with time zone not null default now(),
  unique (source_system, external_ref)
);

create index if not exists idx_bling_nfe_documents_company_issue_date
on public.bling_nfe_documents(company, issue_datetime);

create index if not exists idx_bling_nfe_documents_salesperson
on public.bling_nfe_documents(salesperson_bling_id);

create or replace view public.vw_bling_sales_realized
with (security_invoker = true)
as
select
  doc.id,
  doc.company,
  doc.issue_datetime::date as transaction_date,
  doc.invoice_number,
  doc.total_amount as revenue_amount,
  doc.customer_name,
  doc.customer_tax_id,
  doc.customer_state,
  coalesce(doc.salesperson_bling_id, 'SEM_VENDEDOR') as sales_rep_code,
  coalesce(doc.salesperson_bling_id, 'SEM_VENDEDOR') as sales_rep_name,
  doc.natureza_id as natureza,
  doc.first_cfop as cfop,
  case
    when doc.natureza_id is not null and doc.first_cfop is not null then doc.natureza_id || ' - ' || doc.first_cfop
    when doc.natureza_id is not null then doc.natureza_id
    else coalesce(doc.first_cfop, '')
  end as natureza_label,
  doc.access_key,
  doc.series,
  doc.source_system,
  doc.external_ref,
  doc.payload,
  doc.updated_at
from public.bling_nfe_documents doc;
