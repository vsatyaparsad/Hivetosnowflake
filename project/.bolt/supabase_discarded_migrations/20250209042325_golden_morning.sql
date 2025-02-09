/*
  # Complex SQL Examples Collection
  
  1. New Tables
    - user_sessions: Stores user session data
    - processed_orders: Stores processed order information
    - sales_summary: Stores sales analysis data
    - sales_partitioned: Stores partitioned sales data
    - cleaned_data: Stores deduplicated event data
    - rolling_metrics: Stores rolling averages
    - apac_sales: Stores APAC region sales
    - ml_features: Stores ML feature data

  2. Security
    - RLS enabled on all tables
    - Basic read/write policies added
*/

-- Create user_sessions table
CREATE TABLE IF NOT EXISTS user_sessions (
  user_id UUID REFERENCES auth.users(id),
  session_id BIGINT,
  session_start TIMESTAMP,
  session_end TIMESTAMP,
  events_per_session INTEGER,
  PRIMARY KEY (user_id, session_id)
);

ALTER TABLE user_sessions ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can read own sessions"
  ON user_sessions
  FOR SELECT
  TO authenticated
  USING (auth.uid() = user_id);

CREATE POLICY "Users can insert own sessions"
  ON user_sessions
  FOR INSERT
  TO authenticated
  WITH CHECK (auth.uid() = user_id);

-- Create processed_orders table
CREATE TABLE IF NOT EXISTS processed_orders (
  order_id TEXT PRIMARY KEY,
  customer_id UUID REFERENCES auth.users(id),
  order_total DECIMAL(15,2),
  products_purchased TEXT[]
);

ALTER TABLE processed_orders ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can read own orders"
  ON processed_orders
  FOR SELECT
  TO authenticated
  USING (auth.uid() = customer_id);

-- Create sales_summary table
CREATE TABLE IF NOT EXISTS sales_summary (
  product_category TEXT,
  customer_region TEXT,
  total_sales DECIMAL(15,2),
  PRIMARY KEY (product_category, customer_region)
);

ALTER TABLE sales_summary ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can read sales summary"
  ON sales_summary
  FOR SELECT
  TO authenticated
  USING (true);

-- Create sales_partitioned table
CREATE TABLE IF NOT EXISTS sales_partitioned (
  transaction_id TEXT PRIMARY KEY,
  product_id TEXT,
  amount DECIMAL(15,2),
  customer_id UUID REFERENCES auth.users(id),
  sale_year INTEGER,
  sale_month INTEGER
);

CREATE INDEX idx_sales_partitioned_date 
  ON sales_partitioned (sale_year, sale_month);

ALTER TABLE sales_partitioned ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can read own sales"
  ON sales_partitioned
  FOR SELECT
  TO authenticated
  USING (auth.uid() = customer_id);

-- Create cleaned_data table
CREATE TABLE IF NOT EXISTS cleaned_data (
  user_id UUID REFERENCES auth.users(id),
  event_type TEXT,
  event_time TIMESTAMP,
  device_id TEXT,
  PRIMARY KEY (user_id, event_type, event_time)
);

ALTER TABLE cleaned_data ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can read own events"
  ON cleaned_data
  FOR SELECT
  TO authenticated
  USING (auth.uid() = user_id);

-- Create rolling_metrics table
CREATE TABLE IF NOT EXISTS rolling_metrics (
  product_id TEXT,
  transaction_date DATE,
  rolling_7d_avg DECIMAL(15,2),
  PRIMARY KEY (product_id, transaction_date)
);

ALTER TABLE rolling_metrics ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can read metrics"
  ON rolling_metrics
  FOR SELECT
  TO authenticated
  USING (true);

-- Create apac_sales table
CREATE TABLE IF NOT EXISTS apac_sales (
  transaction_id TEXT PRIMARY KEY,
  customer_segment TEXT,
  amount DECIMAL(15,2)
);

ALTER TABLE apac_sales ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can read APAC sales"
  ON apac_sales
  FOR SELECT
  TO authenticated
  USING (true);

-- Create ml_features table
CREATE TABLE IF NOT EXISTS ml_features (
  store_id TEXT,
  sale_date DATE,
  daily_sales DECIMAL(15,2),
  rolling_7d_avg DECIMAL(15,2),
  prev_week_sales DECIMAL(15,2),
  PRIMARY KEY (store_id, sale_date)
);

ALTER TABLE ml_features ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Authenticated users can read ML features"
  ON ml_features
  FOR SELECT
  TO authenticated
  USING (true);

-- Create functions for session management
CREATE OR REPLACE FUNCTION calculate_session_id(
  p_user_id UUID,
  p_event_time TIMESTAMP
) RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  v_session_id BIGINT;
BEGIN
  SELECT COALESCE(MAX(session_id), 0) + 1
  INTO v_session_id
  FROM user_sessions
  WHERE user_id = p_user_id
    AND p_event_time > session_end + INTERVAL '30 minutes';
  
  RETURN v_session_id;
END;
$$;

-- Create function for rolling averages
CREATE OR REPLACE FUNCTION calculate_rolling_average(
  p_product_id TEXT,
  p_date DATE
) RETURNS DECIMAL(15,2)
LANGUAGE plpgsql
AS $$
DECLARE
  v_avg DECIMAL(15,2);
BEGIN
  SELECT AVG(amount)
  INTO v_avg
  FROM sales_partitioned
  WHERE product_id = p_product_id
    AND sale_date BETWEEN p_date - INTERVAL '6 days' AND p_date;
  
  RETURN v_avg;
END;
$$;