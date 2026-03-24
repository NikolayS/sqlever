ALTER TABLE app.payments ADD CONSTRAINT fk_payments_order
  FOREIGN KEY (order_id) REFERENCES app.orders(id);
