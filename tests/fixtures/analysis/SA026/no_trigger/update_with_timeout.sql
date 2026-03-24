set statement_timeout = '30s';
update users set name = 'unknown' where name is null;
