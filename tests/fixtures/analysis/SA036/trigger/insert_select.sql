INSERT INTO archive_users (id, email, name)
SELECT id, email, name FROM users WHERE deleted_at IS NOT NULL;
