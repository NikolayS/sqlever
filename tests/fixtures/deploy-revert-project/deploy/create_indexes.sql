-- Deploy create_indexes
-- Non-transactional: CREATE INDEX CONCURRENTLY cannot run inside a transaction.

create index concurrently if not exists idx_users_email
    on public.users (email);

create index concurrently if not exists idx_orders_user_id
    on public.orders (user_id);

create index concurrently if not exists idx_orders_status
    on public.orders (status);
