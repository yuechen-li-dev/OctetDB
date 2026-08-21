CREATE TABLE IF NOT EXISTS customers (id BIGINT PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS inventory (product_id BIGINT PRIMARY KEY, sku TEXT NOT NULL UNIQUE, quantity INTEGER NOT NULL CHECK (quantity >= 0));
CREATE TABLE IF NOT EXISTS orders (id BIGINT PRIMARY KEY, customer_id BIGINT NOT NULL REFERENCES customers(id), created_at TIMESTAMPTZ NOT NULL, status TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS orders_customer_recent ON orders (customer_id, created_at DESC);
CREATE TABLE IF NOT EXISTS order_items (order_id BIGINT NOT NULL REFERENCES orders(id) ON DELETE CASCADE, product_id BIGINT NOT NULL REFERENCES inventory(product_id), quantity INTEGER NOT NULL CHECK (quantity > 0), unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0), PRIMARY KEY (order_id, product_id));
