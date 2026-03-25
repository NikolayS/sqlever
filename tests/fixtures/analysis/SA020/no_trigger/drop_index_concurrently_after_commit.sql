BEGIN;
CREATE TABLE t (id int8 generated always as identity PRIMARY KEY);
COMMIT;
DROP INDEX CONCURRENTLY IF EXISTS idx_t_id;
