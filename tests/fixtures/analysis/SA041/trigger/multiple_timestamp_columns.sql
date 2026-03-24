create table audit_log (
  id int8 generated always as identity primary key,
  created_at timestamp,
  updated_at timestamp,
  description text
);
