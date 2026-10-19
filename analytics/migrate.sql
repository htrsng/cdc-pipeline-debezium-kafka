-- Safe to reapply. Run against existing DWH before starting the new consumer.
BEGIN;
CREATE TABLE IF NOT EXISTS pending_order_items (
 order_item_id INT PRIMARY KEY, event JSONB NOT NULL,
 queued_at TIMESTAMP NOT NULL DEFAULT now()
);
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE OR REPLACE VIEW analytics.sales_items AS
SELECT f.order_item_id, f.order_id, f.customer_id, f.product_id,
 d.full_date AS order_date, s.order_date AS ordered_at,
 c.city, p.product_name, p.category, f.quantity, f.unit_price,
 f.total_amount AS order_value,
 s.order_status,
 CASE WHEN s.order_status IN ('CONFIRMED','SHIPPED') THEN f.total_amount ELSE 0 END AS confirmed_value
FROM fact_orders f
JOIN stg_orders s ON s.order_id=f.order_id
JOIN dim_date d ON d.date_id=to_char(s.order_date, 'YYYYMMDD')::int
LEFT JOIN dim_customer c ON c.customer_id=f.customer_id
LEFT JOIN dim_product p ON p.product_id=f.product_id
WHERE NOT f.is_deleted AND NOT s.is_deleted;
CREATE OR REPLACE VIEW analytics.orders AS
SELECT order_id, customer_id, order_date, ordered_at, city, order_status,
 SUM(quantity) AS units, SUM(order_value) AS order_value,
 SUM(confirmed_value) AS confirmed_value
FROM analytics.sales_items
GROUP BY order_id, customer_id, order_date, ordered_at, city, order_status;
CREATE OR REPLACE VIEW analytics.daily_sales AS
SELECT order_date, COUNT(*) AS orders, COUNT(DISTINCT customer_id) AS customers,
 SUM(units) AS units, SUM(order_value) AS order_value,
 SUM(confirmed_value) AS confirmed_value, AVG(order_value) AS average_order_value,
 COUNT(*) FILTER (WHERE order_status='CANCELLED')::numeric / NULLIF(COUNT(*),0) AS cancellation_rate
FROM analytics.orders GROUP BY order_date;
CREATE OR REPLACE VIEW analytics.customer_rfm AS
SELECT customer_id, MAX(city) AS city,
 CURRENT_DATE - MAX(order_date) AS recency_days,
 COUNT(*) AS frequency, SUM(confirmed_value) AS monetary,
 MIN(order_date) AS first_order_date, MAX(order_date) AS last_order_date
FROM analytics.orders WHERE order_status IN ('CONFIRMED','SHIPPED')
GROUP BY customer_id;
-- Event processing latency, not dashboard freshness. Snapshot events (r) excluded.
CREATE OR REPLACE VIEW analytics.cdc_minute AS
SELECT date_trunc('minute', processed_at) AS time,
 COUNT(*) AS processed_events,
 percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM processed_at)*1000-source_ts_ms) AS p50_ms,
 percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM processed_at)*1000-source_ts_ms) AS p95_ms,
 percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM processed_at)*1000-source_ts_ms) AS p99_ms
FROM cdc_audit_log WHERE operation IN ('c','u','d') AND source_ts_ms > 0
GROUP BY 1;
CREATE INDEX IF NOT EXISTS idx_fact_order_id ON fact_orders(order_id);
CREATE INDEX IF NOT EXISTS idx_audit_processed_at ON cdc_audit_log(processed_at);
COMMIT;
