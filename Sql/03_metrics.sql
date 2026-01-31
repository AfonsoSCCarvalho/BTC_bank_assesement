-- Metrics computed from the cleaned views

-- ----------------------------
-- A) Total Volume Transacted
-- ----------------------------
-- Volume is computed on completed transactions only.
-- If multiple currencies exist, we show per-currency and an overall total (no FX conversion).
WITH completed AS (
  SELECT *
  FROM clean_transactions
  WHERE status = 'completed'
)
SELECT
  currency,
  SUM(amount) AS total_volume_transacted,
  COUNT(*) AS completed_txn_count
FROM completed
GROUP BY currency
ORDER BY total_volume_transacted DESC
;

-- Overall total (across currencies, no FX conversion)
WITH completed AS (
  SELECT *
  FROM clean_transactions
  WHERE status = 'completed'
)
SELECT
  SUM(amount) AS total_volume_transacted_all_currencies,
  COUNT(*) AS completed_txn_count
FROM completed
;

-- ----------------------------
-- B) Daily Active Users (DAU)
-- ----------------------------
-- DAU = distinct active users per calendar day based on clean_app_events.
-- We generate the full date range so days with zero activity appear.
-- B) Monthly DAU summary (mean + median of daily DAU across the month)
-- DAU is computed from clean_app_events; we include zero-activity days by generating the full date range.

WITH
bounds AS (
  SELECT
    date(min(created_at), 'start of month') AS window_start,
    date(min(created_at), 'start of month', '+1 month') AS window_end
  FROM clean_transactions
),
dates(d) AS (
  SELECT window_start FROM bounds
  UNION ALL
  SELECT date(d, '+1 day')
  FROM dates, bounds
  WHERE d < date(window_end, '-1 day')
),
active AS (
  SELECT
    date(event_ts) AS d,
    COUNT(DISTINCT user_id) AS dau
  FROM clean_app_events
  GROUP BY date(event_ts)
),
daily AS (
  SELECT
    dates.d AS date,
    COALESCE(active.dau, 0) AS dau
  FROM dates
  LEFT JOIN active ON active.d = dates.d
),
median_calc AS (
  -- Median over the daily DAU distribution (works for both odd and even number of days)
  SELECT AVG(dau) AS median_dau
  FROM (
    SELECT
      dau,
      ROW_NUMBER() OVER (ORDER BY dau) AS rn,
      COUNT(*) OVER () AS n
    FROM daily
  )
  WHERE rn IN (
    CAST((n + 1) / 2 AS INTEGER),
    CAST((n + 2) / 2 AS INTEGER)
  )
)

SELECT
  b.window_start AS month_start,
  date(b.window_end, '-1 day') AS month_end,
  AVG(d.dau) AS mean_daily_dau,
  m.median_dau AS median_daily_dau
FROM daily d
CROSS JOIN bounds b
CROSS JOIN median_calc m
;

-- ----------------------------
-- C) Average Transaction Size per User
-- ----------------------------
-- Interpretation: per user (sender), compute mean transaction amount (completed),
-- then take the average across users (average-of-user-averages).
WITH per_user AS (
  SELECT
    sender_user_id AS user_id,
    COUNT(*) AS completed_txn_count,
    SUM(amount) AS total_sent_amount,
    AVG(amount) AS avg_txn_size_user
  FROM clean_transactions
  WHERE status = 'completed'
  GROUP BY sender_user_id
)
SELECT
  AVG(avg_txn_size_user) AS avg_transaction_size_per_user,
  COUNT(*) AS users_with_completed_txns
FROM per_user
;

