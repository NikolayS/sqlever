-- Revert create_orders
BEGIN;
drop table if exists public.orders cascade;
COMMIT;
