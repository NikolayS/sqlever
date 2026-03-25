-- Verify create_indexes
select 1 from pg_indexes where indexname = 'idx_users_email';
select 1 from pg_indexes where indexname = 'idx_orders_user_id';
select 1 from pg_indexes where indexname = 'idx_orders_status';
