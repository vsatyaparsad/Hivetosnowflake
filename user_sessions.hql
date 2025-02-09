-- User Sessionization with Window Functions
-- =====================================================================
-- Complex Hive HQL Examples Collection
-- Includes: Sessionization, JSON handling, Optimized Joins, Partitioning
--           Deduplication, Rolling Aggregates, Skew Handling, ML Features
-- =====================================================================

-- #####################################################################
-- 1. User Sessionization with Window Functions (30-minute inactivity)
-- #####################################################################
SET hive.exec.parallel=true;

WITH event_sequence AS (
  SELECT
    user_id,
    event_time,
    event_name,
    LAG(event_time, 1) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_event_time
  FROM user_clickstream
)
INSERT OVERWRITE TABLE user_sessions
SELECT
  user_id,
  session_id,
  MIN(event_time) AS session_start,
  MAX(event_time) AS session_end,
  COUNT(*) AS events_per_session
FROM (
  SELECT
    user_id,
    event_time,
    SUM(session_flag) OVER (PARTITION BY user_id ORDER BY event_time) AS session_id
  FROM (
    SELECT
      user_id,
      event_time,
      CASE WHEN unix_timestamp(event_time) - unix_timestamp(prev_event_time) > 1800 THEN 1 ELSE 0 END AS session_flag
    FROM event_sequence
  ) flagged_events
) sessionized
GROUP BY user_id, session_id;


-- #####################################################################
-- 2. Nested JSON Processing with Explode and Aggregation
-- #####################################################################
CREATE TABLE IF NOT EXISTS processed_orders (
  order_id STRING,
  customer_id STRING,
  order_total DOUBLE,
  products_purchased ARRAY<STRING>
);

INSERT INTO TABLE processed_orders
SELECT
  order_id,
  customer_id,
  SUM(product.total_price) AS order_total,
  COLLECT_SET(product.product_id) AS products_purchased
FROM (
  SELECT
    order_id,
    customer_id,
    explode(order_items) AS product
  FROM orders_json
  LATERAL VIEW JSON_TUPLE(raw_json, 'order_id', 'customer_id', 'items') jt AS order_id, customer_id, order_items
) unnested
GROUP BY order_id, customer_id;


-- #####################################################################
-- 3. Optimized Multi-Source Sales Analysis with UNION and Map Joins
-- #####################################################################
SET hive.auto.convert.join=true;

CREATE TABLE sales_summary (
  product_category STRING,
  customer_region STRING,
  total_sales DOUBLE
) STORED AS ORC;

INSERT INTO TABLE sales_summary
SELECT product_category, customer_region, SUM(amount) AS total_sales
FROM (
  SELECT /*+ MAPJOIN(p) */ 
    s.transaction_id, p.product_category, c.customer_region, s.amount
  FROM online_transactions s
  JOIN products p ON s.product_id = p.product_id
  LEFT JOIN customers c ON s.customer_id = c.customer_id
  WHERE s.sale_date BETWEEN '2023-01-01' AND '2023-01-31'
  
  UNION ALL
  
  SELECT /*+ MAPJOIN(p) */ 
    s.receipt_id, p.product_category, 'In-Store' AS customer_region, s.amount
  FROM physical_transactions s
  JOIN products p ON s.product_id = p.product_id
  WHERE s.sale_date BETWEEN '2023-01-01' AND '2023-01-31'
) combined
GROUP BY product_category, customer_region
CLUSTER BY product_category;


-- #####################################################################
-- 4. Dynamic Partitioning with Bucketed Sales Data
-- #####################################################################
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE sales_partitioned (
  transaction_id STRING,
  product_id STRING,
  amount DOUBLE,
  customer_id STRING
)
PARTITIONED BY (sale_year INT, sale_month INT)
CLUSTERED BY (product_id) INTO 24 BUCKETS;

INSERT OVERWRITE TABLE sales_partitioned
PARTITION (sale_year, sale_month)
SELECT
  transaction_id,
  product_id,
  amount,
  customer_id,
  year(sale_date) AS sale_year,
  month(sale_date) AS sale_month
FROM raw_sales
DISTRIBUTE BY sale_year, sale_month
SORT BY product_id;


-- #####################################################################
-- 5. Deduplication with Row Numbering
-- #####################################################################
CREATE TABLE cleaned_data (
  user_id STRING,
  event_type STRING,
  event_time TIMESTAMP,
  device_id STRING
) STORED AS PARQUET;

INSERT OVERWRITE TABLE cleaned_data
SELECT
  user_id,
  event_type,
  event_time,
  device_id
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY user_id, event_type
      ORDER BY event_time DESC
    ) AS row_num
  FROM raw_events
) ranked
WHERE row_num = 1;


-- #####################################################################
-- 6. 7-Day Rolling Average Calculation
-- #####################################################################
CREATE TABLE rolling_metrics (
  product_id STRING,
  transaction_date DATE,
  rolling_7d_avg DOUBLE
);

INSERT INTO TABLE rolling_metrics
SELECT
  product_id,
  transaction_date,
  AVG(daily_revenue) OVER (
    PARTITION BY product_id
    ORDER BY unix_timestamp(transaction_date)
    RANGE BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7d_avg
FROM (
  SELECT
    product_id,
    sale_date AS transaction_date,
    SUM(amount) AS daily_revenue
  FROM sales
  GROUP BY product_id, sale_date
) daily_agg;


-- #####################################################################
-- 7. Skew Join Optimization for Customer Data
-- #####################################################################
SET hive.optimize.skewjoin=true;
SET hive.skewjoin.key=100000;

CREATE TABLE apac_sales (
  transaction_id STRING,
  customer_segment STRING,
  amount DOUBLE
);

INSERT INTO TABLE apac_sales
SELECT /*+ SKEWJOIN(c) */
  s.transaction_id,
  c.customer_segment,
  s.amount
FROM sales s
JOIN customers c
ON s.customer_id = c.customer_id
WHERE c.customer_region = 'APAC';


-- #####################################################################
-- 8. Time-Series Feature Engineering for ML
-- #####################################################################
CREATE TABLE ml_features (
  store_id STRING,
  sale_date DATE,
  daily_sales DOUBLE,
  rolling_7d_avg DOUBLE,
  prev_week_sales DOUBLE
);

INSERT OVERWRITE TABLE ml_features
SELECT
  store_id,
  sale_date,
  daily_sales,
  AVG(daily_sales) OVER (
    PARTITION BY store_id
    ORDER BY sale_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7d_avg,
  LAG(daily_sales, 7) OVER (PARTITION BY store_id ORDER BY sale_date) AS prev_week_sales
FROM (
  SELECT
    store_id,
    sale_date,
    SUM(amount) AS daily_sales
  FROM sales
  GROUP BY store_id, sale_date
) daily;