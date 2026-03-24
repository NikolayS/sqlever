CREATE TABLE users (
  id int8 generated always as identity primary key,
  email text not null
);
