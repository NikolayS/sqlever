-- Revert create_users
BEGIN;
drop table if exists public.users cascade;
COMMIT;
