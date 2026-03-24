set statement_timeout = '60s';
delete from users where active = false;
