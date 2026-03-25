-- Revert create_indexes
drop index concurrently if exists public.idx_users_email;
drop index concurrently if exists public.idx_orders_user_id;
drop index concurrently if exists public.idx_orders_status;
