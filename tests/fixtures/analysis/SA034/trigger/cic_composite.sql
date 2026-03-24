CREATE INDEX CONCURRENTLY idx_orders_user_date ON orders (user_id, created_at);
