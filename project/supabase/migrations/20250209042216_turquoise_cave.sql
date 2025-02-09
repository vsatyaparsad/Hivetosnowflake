-- =====================================================================
-- Complex Snowflake SQL Examples Collection
-- Includes: Sessionization, JSON handling, Optimized Joins, Partitioning
--           Deduplication, Rolling Aggregates, ML Features
-- =====================================================================

-- #####################################################################
-- 1. User Sessionization with Window Functions (30-minute inactivity)
-- #####################################################################

WITH event_sequence AS (
  SELECT
    user_id,
    event_time,
    event_name,
    LAG(event_time, 1) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_event_time
  FROM user_clickstream
)
INSERT OVERWRITE INTO user_sessions
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
      CASE WHEN to_number(to_timestamp_ntz(event_time), 38, 0) - to_number(to_timestamp_ntz(prev_event_time), 38, 0) > 1800 THEN 1 ELSE 0 END AS session_flag
    FROM event_sequence
  ) flagged_events
) sessionized
GROUP BY user_id, session_id;


-- #####################################################################
-- 2. Nested JSON Processing with Flattening and Aggregation
-- #####################################################################
CREATE OR REPLACE TABLE processed_orders (
  order_id VARCHAR,
  customer_id VARCHAR,
  order_total DOUBLE,
  products_purchased ARRAY
);

INSERT INTO processed_orders
SELECT
  order_id,
  customer_id,
  SUM(product.total_price) AS order_total,
  array_agg(DISTINCT product.product_id) AS products_purchased
FROM (
  SELECT
    get_path(parse_json(raw_json), 'order_id') AS order_id,
    get_path(parse_json(raw_json), 'customer_id') AS customer_id,
    f.value AS product
  FROM orders_json,
  LATERAL FLATTEN(input => parse_json(raw_json):items) f
) unnested
GROUP BY order_id, customer_id;


-- #####################################################################
-- 3. Multi-Source Sales Analysis with UNION
-- #####################################################################
CREATE OR REPLACE TABLE sales_summary (
  product_category VARCHAR,
  customer_region VARCHAR,
  total_sales DOUBLE
);

INSERT INTO sales_summary
SELECT product_category, customer_region, SUM(amount) AS total_sales
FROM (
  SELECT 
    s.transaction_id, p.product_category, c.customer_region, s.amount
  FROM online_transactions s
  JOIN products p ON s.product_id = p.product_id
  LEFT JOIN customers c ON s.customer_id = c.customer_id
  WHERE s.sale_date BETWEEN '2023-01-01' AND '2023-01-31'
  
  UNION ALL
  
  SELECT 
    s.receipt_id, p.product_category, 'In-Store' AS customer_region, s.amount
  FROM physical_transactions s
  JOIN products p ON s.product_id = p.product_id
  WHERE s.sale_date BETWEEN '2023-01-01' AND '2023-01-31'
) combined
GROUP BY product_category, customer_region
ORDER BY product_category;


-- #####################################################################
-- 4. Sales Data with Clustering
-- #####################################################################
CREATE OR REPLACE TABLE sales_partitioned (
  transaction_id VARCHAR,
  product_id VARCHAR,
  amount DOUBLE,
  customer_id VARCHAR,
  sale_year NUMBER(4,0),
  sale_month NUMBER(2,0)
)
CLUSTER BY (sale_year, sale_month, product_id);

INSERT OVERWRITE INTO sales_partitioned
SELECT
  transaction_id,
  product_id,
  amount,
  customer_id,
  year(sale_date) AS sale_year,
  month(sale_date) AS sale_month
FROM raw_sales
ORDER BY sale_year, sale_month, product_id;


-- #####################################################################
-- 5. Deduplication with Row Numbering
-- #####################################################################
CREATE OR REPLACE TABLE cleaned_data (
  user_id VARCHAR,
  event_type VARCHAR,
  event_time TIMESTAMP_NTZ,
  device_id VARCHAR
);

INSERT OVERWRITE INTO cleaned_data
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
CREATE OR REPLACE TABLE rolling_metrics (
  product_id VARCHAR,
  transaction_date DATE,
  rolling_7d_avg DOUBLE
);

INSERT INTO rolling_metrics
SELECT
  product_id,
  transaction_date,
  AVG(daily_revenue) OVER (
    PARTITION BY product_id
    ORDER BY transaction_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
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
-- 7. Sales Analysis with Optimized Joins
-- #####################################################################
CREATE OR REPLACE TABLE apac_sales (
  transaction_id VARCHAR,
  customer_segment VARCHAR,
  amount DOUBLE
);

INSERT INTO apac_sales
SELECT
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
CREATE OR REPLACE TABLE ml_features (
  store_id VARCHAR,
  sale_date DATE,
  daily_sales DOUBLE,
  rolling_7d_avg DOUBLE,
  prev_week_sales DOUBLE
);

INSERT OVERWRITE INTO ml_features
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