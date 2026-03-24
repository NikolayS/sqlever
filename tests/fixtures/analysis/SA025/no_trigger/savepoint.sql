-- SAVEPOINT is fine inside a migration
savepoint sp1;
alter table users add column bio text;
release savepoint sp1;
