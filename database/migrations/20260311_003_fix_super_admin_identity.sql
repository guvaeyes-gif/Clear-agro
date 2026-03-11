-- Clear OS / Supabase
-- Migration: 20260311_003_fix_super_admin_identity.sql
-- Purpose: ensure Cesar user exists as super_admin.
-- If auth.users has gmail login, link auth_user_id. Otherwise keep pending linkage.

-- remove old placeholder seed row if it exists without auth link
DELETE FROM public.app_users
WHERE lower(email) = lower('cesar@example.com')
  AND auth_user_id IS NULL;

DO $$
DECLARE
  v_auth_user_id uuid;
BEGIN
  SELECT au.id INTO v_auth_user_id
  FROM auth.users au
  WHERE lower(au.email) = lower('czarovski@gmail.com')
  ORDER BY au.created_at
  LIMIT 1;

  IF v_auth_user_id IS NOT NULL THEN
    -- 1) update row already linked to this auth user, if exists
    UPDATE public.app_users
    SET
      full_name = 'Cesar Zarovski',
      email = 'cesar@clearagro.com.br',
      role = 'super_admin'::app_role,
      department = 'Diretoria',
      is_active = true,
      status = 'active'::record_status,
      metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object(
        'business_email', 'cesar@clearagro.com.br',
        'login_email', 'czarovski@gmail.com',
        'auth_linked', true,
        'seed_source', '20260311_003_fix_super_admin_identity'
      ),
      updated_at = now()
    WHERE auth_user_id = v_auth_user_id;

    -- 2) if not linked yet, upsert corporate email and bind auth id
    IF NOT FOUND THEN
      INSERT INTO public.app_users (
        auth_user_id,
        full_name,
        email,
        role,
        department,
        is_active,
        status,
        metadata
      )
      VALUES (
        v_auth_user_id,
        'Cesar Zarovski',
        'cesar@clearagro.com.br',
        'super_admin'::app_role,
        'Diretoria',
        true,
        'active'::record_status,
        jsonb_build_object(
          'business_email', 'cesar@clearagro.com.br',
          'login_email', 'czarovski@gmail.com',
          'auth_linked', true,
          'seed_source', '20260311_003_fix_super_admin_identity'
        )
      )
      ON CONFLICT (email) DO UPDATE
      SET
        auth_user_id = COALESCE(public.app_users.auth_user_id, EXCLUDED.auth_user_id),
        full_name = EXCLUDED.full_name,
        role = EXCLUDED.role,
        department = EXCLUDED.department,
        is_active = EXCLUDED.is_active,
        status = EXCLUDED.status,
        metadata = COALESCE(public.app_users.metadata, '{}'::jsonb) || EXCLUDED.metadata,
        updated_at = now();
    END IF;
  ELSE
    -- No auth login found yet: keep business super admin ready, pending auth linkage.
    INSERT INTO public.app_users (
      auth_user_id,
      full_name,
      email,
      role,
      department,
      is_active,
      status,
      metadata
    )
    VALUES (
      null,
      'Cesar Zarovski',
      'cesar@clearagro.com.br',
      'super_admin'::app_role,
      'Diretoria',
      true,
      'active'::record_status,
      jsonb_build_object(
        'business_email', 'cesar@clearagro.com.br',
        'login_email', 'czarovski@gmail.com',
        'auth_linked', false,
        'seed_source', '20260311_003_fix_super_admin_identity'
      )
    )
    ON CONFLICT (email) DO UPDATE
    SET
      full_name = EXCLUDED.full_name,
      role = EXCLUDED.role,
      department = EXCLUDED.department,
      is_active = EXCLUDED.is_active,
      status = EXCLUDED.status,
      metadata = COALESCE(public.app_users.metadata, '{}'::jsonb) || EXCLUDED.metadata,
      updated_at = now();
  END IF;
END $$;
