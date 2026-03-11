-- Clear OS / Supabase
-- Migration: 20260311_004_link_auth_user_cesar.sql
-- Purpose: link login auth user (Gmail) to business super admin row.

UPDATE public.app_users au
SET
  auth_user_id = u.id,
  metadata = COALESCE(au.metadata, '{}'::jsonb) || jsonb_build_object(
    'auth_linked', true,
    'login_email', 'czarovski@gmail.com',
    'linked_at', now(),
    'link_source', '20260311_004_link_auth_user_cesar'
  ),
  updated_at = now()
FROM auth.users u
WHERE lower(u.email) = lower('czarovski@gmail.com')
  AND lower(au.email) = lower('cesar@clearagro.com.br')
  AND (au.auth_user_id IS NULL OR au.auth_user_id <> u.id);
