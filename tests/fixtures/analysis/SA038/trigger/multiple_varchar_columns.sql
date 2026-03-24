create table products (
  id int8 generated always as identity primary key,
  name varchar(200),
  sku varchar(50),
  description text
);
