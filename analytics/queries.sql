-- Overview: one row per order, exclude cancelled/pending when calculating confirmed AOV.
SELECT COUNT(*) AS orders,
       SUM(order_value) AS order_value,
       SUM(confirmed_value) AS confirmed_value,
       AVG(order_value) AS average_order_value,
       AVG(order_value) FILTER (WHERE order_status IN ('CONFIRMED','SHIPPED')) AS confirmed_aov
FROM analytics.orders;

-- Product ranking, including cancelled items with zero confirmed value.
SELECT product_id, product_name, category,
       SUM(confirmed_value) AS confirmed_value,
       COUNT(DISTINCT order_id) AS orders
FROM analytics.sales_items
GROUP BY product_id, product_name, category
ORDER BY confirmed_value DESC LIMIT 20;

-- Customer repeat rate among customers with confirmed orders.
SELECT COUNT(*) AS customers,
       COUNT(*) FILTER (WHERE frequency > 1)::numeric / NULLIF(COUNT(*),0) AS repeat_customer_rate
FROM analytics.customer_rfm;

SELECT order_status, COUNT(*) AS orders, SUM(order_value) AS order_value
FROM analytics.orders GROUP BY order_status;

-- Products purchased together: one pair per order even if a product has multiple items.
WITH basket AS (
 SELECT DISTINCT order_id, product_id FROM analytics.sales_items
 WHERE order_status IN ('CONFIRMED','SHIPPED')
)
SELECT a.product_id AS product_a, b.product_id AS product_b, COUNT(*) AS orders_together
FROM basket a JOIN basket b ON a.order_id=b.order_id AND a.product_id<b.product_id
GROUP BY a.product_id,b.product_id ORDER BY orders_together DESC LIMIT 20;
