-- Deploy create_users

BEGIN;

create table public.users (
    id          int8 generated always as identity primary key,
    username    text not null unique,
    email       text not null,
    created_at  timestamptz not null default clock_timestamp()
);

COMMIT;
