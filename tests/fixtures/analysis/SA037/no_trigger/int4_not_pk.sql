CREATE TABLE orders (
  id int8 generated always as identity primary key,
  quantity int4 not null
);
