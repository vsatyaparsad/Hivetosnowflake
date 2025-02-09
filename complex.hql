-- Analyze user transaction sequences to detect potential fraud patterns
WITH transaction_graph AS (
  SELECT
    user_id,
    transaction_time,
    amount,
    country,
    merchant_type,
    -- Create session IDs with 1-hour inactivity windows
    SUM(session_flag) OVER (
      PARTITION BY user_id 
      ORDER BY unix_timestamp(transaction_time)
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS session_id,
    -- Calculate velocity metrics
    COUNT(*) OVER (
      PARTITION BY user_id 
      ORDER BY unix_timestamp(transaction_time)
      RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW
    ) AS transactions_24h,
    SUM(amount) OVER (
      PARTITION BY user_id 
      ORDER BY unix_timestamp(transaction_time)
      ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS rolling_5txn_sum
  FROM (
    SELECT
      *,
      CASE 
        WHEN unix_timestamp(transaction_time) - 
             LAG(unix_timestamp(transaction_time), 1, 0) OVER w > 3600 
        THEN 1 
        ELSE 0 
      END AS session_flag
    FROM transactions
    WINDOW w AS (PARTITION BY user_id ORDER BY transaction_time)
  ) sessionized
),

pattern_analysis AS (
  SELECT
    user_id,
    session_id,
    TRANSFORM(
      COLLECT_LIST(
        STRUCT(
          transaction_time,
          amount,
          country,
          merchant_type
        )
      ) OVER (
        PARTITION BY user_id, session_id 
        ORDER BY transaction_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ),
      t -> CASE
        WHEN t.amount > 10000 THEN 'H'
        WHEN t.amount BETWEEN 5000 AND 10000 THEN 'M'
        ELSE 'L'
      END
    ) AS amount_pattern,
    CORR(
      DENSE_RANK() OVER (ORDER BY transaction_time),
      amount
    ) AS time_amount_correlation,
    STDDEV_POP(amount) OVER (
      PARTITION BY user_id, session_id
    ) AS amount_stddev,
    COUNT(DISTINCT country) OVER (
      PARTITION BY user_id, session_id
      ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS distinct_countries_5txn
  FROM transaction_graph
),

fraud_features AS (
  SELECT
    user_id,
    session_id,
    MAX(rolling_5txn_sum) AS max_5txn_sum,
    MIN(transactions_24h) AS min_txn_24h,
    -- Detect pattern sequences using regular expressions
    CASE
      WHEN REGEXP_REPLACE(
           CONCAT_WS('', amount_pattern), 
           '(H){3,}|(LHL){2,}|M{5}', 
           '**'
         ) != CONCAT_WS('', amount_pattern)
      THEN 1
      ELSE 0
    END AS suspicious_pattern,
    CASE
      WHEN time_amount_correlation < -0.7 
           AND amount_stddev > 5000 
           AND distinct_countries_5txn >= 3
      THEN 1
      ELSE 0
    END AS velocity_anomaly
  FROM pattern_analysis
  GROUP BY
    user_id,
    session_id,
    amount_pattern,
    time_amount_correlation,
    amount_stddev,
    distinct_countries_5txn
)

-- Final fraud scoring with ML-like features
INSERT OVERWRITE TABLE fraud_scores
PARTITION (risk_category)
SELECT
  f.user_id,
  t.session_id,
  -- Fraud score calculation
  (0.4 * f.suspicious_pattern) + 
  (0.6 * f.velocity_anomaly) + 
  (0.2 * LOG(1 + COALESCE(u.avg_txn_amount, 0))) AS fraud_score,
  CASE
    WHEN (f.suspicious_pattern + f.velocity_anomaly) >= 2 THEN 'high_risk'
    WHEN (f.suspicious_pattern + f.velocity_anomaly) = 1 THEN 'medium_risk'
    ELSE 'low_risk'
  END AS risk_category
FROM fraud_features f
JOIN user_profiles u 
  ON f.user_id = u.user_id
CLUSTER BY risk_category, fraud_score DESC;