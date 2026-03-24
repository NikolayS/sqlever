CREATE TABLE orders (
  id int8 generated always as identity primary key,
  user_id int8 REFERENCES users(id)
);
