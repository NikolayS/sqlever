-- Deploy create_orders

BEGIN;

create table public.orders (
    id          int8 generated always as identity primary key,
    user_id     int8 not null references public.users(id) on delete cascade,
    total_cents int not null default 0,
    status      text not null default 'pending',
    created_at  timestamptz not null default clock_timestamp()
);

COMMIT;
