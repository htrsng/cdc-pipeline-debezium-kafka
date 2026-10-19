CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    full_name VARCHAR(150),
    email VARCHAR(150),
    city VARCHAR(100),
    valid_from TIMESTAMP NOT NULL DEFAULT now(),
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT true
);
CREATE TABLE dim_product (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(150),
    category VARCHAR(100),
    unit_price NUMERIC(12, 2)
);
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    quarter INT,
    year INT
);
CREATE TABLE fact_orders (
    order_item_id INT PRIMARY KEY,
    order_id INT,
    customer_id INT REFERENCES dim_customer(customer_id),
    product_id INT REFERENCES dim_product(product_id),
    date_id INT REFERENCES dim_date(date_id),
    quantity INT,
    unit_price NUMERIC(12, 2),
    total_amount NUMERIC(14, 2),
    order_status VARCHAR(20),
    source_updated_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT now(),
    is_deleted BOOLEAN DEFAULT false
);
CREATE TABLE cdc_audit_log (
    id SERIAL PRIMARY KEY,
    kafka_topic VARCHAR(200),
    kafka_offset BIGINT,
    operation VARCHAR(10),
    table_name VARCHAR(100),
    record_key VARCHAR(100),
    source_ts_ms BIGINT,
    processed_at TIMESTAMP DEFAULT now()
);
-- Bảng đệm (staging) lưu dữ liệu orders mới nhất, dùng để join khi
-- consumer xử lý message order_items
CREATE TABLE stg_orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_status VARCHAR(20),
    order_date TIMESTAMP,
    is_deleted BOOLEAN DEFAULT false,
    updated_at TIMESTAMP DEFAULT now()
);
CREATE TABLE IF NOT EXISTS pending_order_items (
    order_item_id INT PRIMARY KEY,
    event JSONB NOT NULL,
    queued_at TIMESTAMP NOT NULL DEFAULT now()
);