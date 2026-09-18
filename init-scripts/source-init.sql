CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    full_name VARCHAR(150) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    city VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(150) NOT NULL,
    category VARCHAR(100),
    unit_price NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(customer_id),
    order_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    order_date TIMESTAMP NOT NULL DEFAULT now(),
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders(order_id),
    product_id INT NOT NULL REFERENCES products(product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(12, 2) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT now()
);
-- Bắt buộc để Debezium lấy được đầy đủ giá trị "before" khi UPDATE/DELETE
ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
INSERT INTO customers (full_name, email, city)
VALUES ('Nguyen Van A', 'a.nguyen@example.com', 'Ha Noi'),
    (
        'Tran Thi B',
        'b.tran@example.com',
        'Ho Chi Minh'
    ),
    (
        'Le Van C',
        'c.le@example.com',
        'Da Nang'
    );
INSERT INTO products (product_name, category, unit_price)
VALUES ('Ao thun', 'Thoi trang', 150000),
    ('Quan jean', 'Thoi trang', 350000),
    ('Tai nghe', 'Dien tu', 500000),
    ('Ban phim', 'Dien tu', 750000);
INSERT INTO orders (customer_id, order_status)
VALUES (1, 'CONFIRMED'),
    (2, 'PENDING');
INSERT INTO order_items (order_id, product_id, quantity, unit_price)
VALUES (1, 1, 2, 150000),
    (1, 3, 1, 500000),
    (2, 2, 1, 350000);