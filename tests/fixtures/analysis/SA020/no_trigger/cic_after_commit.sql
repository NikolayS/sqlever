BEGIN;
CREATE TABLE t (id int8 generated always as identity PRIMARY KEY);
COMMIT;
CREATE INDEX CONCURRENTLY idx_t_id ON t (id);
