-- Apply migrate.sql first. Reapplicable; no business rows modified.
BEGIN;
CREATE OR REPLACE VIEW analytics.monthly_sales AS
WITH calendar AS (
 SELECT generate_series(date_trunc('month', min(order_date))::date,
                        date_trunc('month', CURRENT_DATE)::date, interval '1 month')::date AS month
 FROM analytics.orders
), totals AS (
 SELECT date_trunc('month', order_date)::date AS month,
 COUNT(*) AS orders, SUM(confirmed_value) AS confirmed_value
 FROM analytics.orders GROUP BY 1
), series AS (
 SELECT c.month, COALESCE(t.orders,0) AS orders,
 COALESCE(t.confirmed_value,0) AS confirmed_value,
 c.month=date_trunc('month',CURRENT_DATE)::date AS is_partial_month
 FROM calendar c LEFT JOIN totals t USING(month)
), comparison AS (
 SELECT *, lag(confirmed_value) OVER (ORDER BY month) AS previous_value FROM series
)
SELECT *, (confirmed_value-previous_value)/NULLIF(previous_value,0) AS growth_rate
FROM comparison;

-- All-time ranking: use sales_items directly for date-filtered rankings.
CREATE OR REPLACE VIEW analytics.product_performance AS
WITH totals AS (
 SELECT product_id, product_name, category,
 COUNT(DISTINCT order_id) AS confirmed_orders, SUM(quantity) AS units,
 SUM(confirmed_value) AS confirmed_value
 FROM analytics.sales_items WHERE order_status IN ('CONFIRMED','SHIPPED')
 GROUP BY product_id, product_name, category
)
SELECT *, confirmed_value/NULLIF(SUM(confirmed_value) OVER (),0) AS value_share,
 SUM(confirmed_value) OVER (ORDER BY confirmed_value DESC,product_id ROWS UNBOUNDED PRECEDING)
 /NULLIF(SUM(confirmed_value) OVER (),0) AS cumulative_share,
 row_number() OVER (ORDER BY confirmed_value DESC,product_id) AS product_rank
FROM totals;

CREATE OR REPLACE VIEW analytics.product_pairs AS
WITH basket AS (
 SELECT DISTINCT order_id,product_id FROM analytics.sales_items
 WHERE order_status IN ('CONFIRMED','SHIPPED')
), pairs AS (
 SELECT a.product_id AS product_a, b.product_id AS product_b, COUNT(*) AS orders_together
 FROM basket a JOIN basket b ON a.order_id=b.order_id AND a.product_id<b.product_id
 GROUP BY a.product_id,b.product_id
)
SELECT pairs.*, a.product_name AS product_a_name,b.product_name AS product_b_name
FROM pairs LEFT JOIN dim_product a ON a.product_id=product_a
LEFT JOIN dim_product b ON b.product_id=product_b;

-- Explicit demo rules, not a trained or quantile-based segmentation model.
CREATE OR REPLACE VIEW analytics.customer_segments AS
SELECT r.*, c.full_name,
 CASE WHEN recency_days>90 THEN 'Inactive >90 days'
      WHEN recency_days>30 THEN 'At risk 31-90 days'
      WHEN frequency>=5 THEN 'Frequent buyers'
      WHEN frequency>=2 THEN 'Repeat buyers'
      ELSE 'One-time buyers' END AS segment
FROM analytics.customer_rfm r LEFT JOIN dim_customer c USING(customer_id);

-- Retention by calendar month, observed months only; includes zero activity.
CREATE OR REPLACE VIEW analytics.customer_cohorts AS
WITH activity AS (
 SELECT DISTINCT customer_id,date_trunc('month',order_date)::date AS month
 FROM analytics.orders WHERE order_status IN ('CONFIRMED','SHIPPED')
), first_purchase AS (
 SELECT customer_id,MIN(month) AS cohort_month FROM activity GROUP BY customer_id
), sizes AS (
 SELECT cohort_month,COUNT(*) AS cohort_size FROM first_purchase GROUP BY cohort_month
), observed AS (
 SELECT s.*, generate_series(s.cohort_month,date_trunc('month',CURRENT_DATE)::date,
                             interval '1 month')::date AS activity_month FROM sizes s
), counts AS (
 SELECT f.cohort_month,a.month AS activity_month,COUNT(*) AS active_customers
 FROM first_purchase f JOIN activity a USING(customer_id) GROUP BY 1,2
)
SELECT o.cohort_month,o.activity_month,
 ((extract(year FROM o.activity_month)-extract(year FROM o.cohort_month))*12
   +extract(month FROM o.activity_month)-extract(month FROM o.cohort_month))::int AS month_number,
 o.cohort_size,COALESCE(c.active_customers,0) AS active_customers,
 COALESCE(c.active_customers,0)::numeric/NULLIF(o.cohort_size,0) AS retention_rate,
 o.activity_month=date_trunc('month',CURRENT_DATE)::date AS is_partial_month
FROM observed o LEFT JOIN counts c USING(cohort_month,activity_month);
COMMIT;
