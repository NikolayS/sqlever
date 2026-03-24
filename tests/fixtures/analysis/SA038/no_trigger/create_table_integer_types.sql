create table counters (
  id int8 generated always as identity primary key,
  value int4,
  total bigint
);
