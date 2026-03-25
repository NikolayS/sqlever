BEGIN;
CREATE TABLE t (id int8 generated always as identity PRIMARY KEY);
ROLLBACK;
CREATE INDEX CONCURRENTLY idx_t_id ON t (id);
