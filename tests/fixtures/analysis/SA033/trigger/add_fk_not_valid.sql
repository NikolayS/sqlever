ALTER TABLE invoices ADD CONSTRAINT fk_invoices_customer
  FOREIGN KEY (customer_id) REFERENCES customers(id) NOT VALID;
