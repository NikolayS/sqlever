CREATE INDEX CONCURRENTLY idx_users_active ON users (email) WHERE active = true;
