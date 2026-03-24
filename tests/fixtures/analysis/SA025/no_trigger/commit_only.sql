-- COMMIT without BEGIN is harmless
alter table users add column bio text;
commit;
