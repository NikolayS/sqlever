insert into users_archive (id, name)
select id, name from users where deleted_at is not null;
