BEGIN;
ALTER TYPE status ADD VALUE 'archived';
ALTER TYPE status ADD VALUE 'deleted';
COMMIT;
