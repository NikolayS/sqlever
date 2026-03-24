CREATE TABLE sessions (
  id uuid primary key default gen_random_uuid(),
  user_id int8 not null
);
