-- Clear OS / Supabase
-- Seed: 20260310_002_phase1a_seed_super_admin.sql
-- Adjust the email below to the real login used in Supabase Auth.

insert into public.app_users (
  auth_user_id,
  full_name,
  email,
  role,
  department,
  is_active,
  status,
  metadata
)
select
  au.id,
  'Cesar Zarovski'::text,
  au.email,
  'super_admin'::app_role,
  'Diretoria'::text,
  true,
  'active'::record_status,
  jsonb_build_object('seeded_at', now(), 'source', '20260310_002')
from auth.users au
where lower(au.email) = lower('cesar@example.com')
on conflict (email) do update
set
  auth_user_id = excluded.auth_user_id,
  full_name = excluded.full_name,
  role = excluded.role,
  is_active = excluded.is_active,
  status = excluded.status,
  updated_at = now();
