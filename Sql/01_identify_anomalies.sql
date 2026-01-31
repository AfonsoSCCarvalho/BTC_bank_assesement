-- Identify anomalies (ordered to match the generator printout)

WITH
bounds AS (
  SELECT
    date(min(created_at), 'start of month') AS window_start,
    date(min(created_at), 'start of month', '+1 month') AS window_end
  FROM transactions
  WHERE created_at IS NOT NULL
),
dup_txn AS (
  SELECT transaction_id, COUNT(*) AS cnt
  FROM transactions
  WHERE transaction_id IS NOT NULL
  GROUP BY transaction_id
  HAVING COUNT(*) > 1
),
txn_before_signup AS (
  SELECT t.transaction_id
  FROM transactions t
  LEFT JOIN users su ON su.user_id = t.sender_user_id
  LEFT JOIN users ru ON ru.user_id = t.receiver_user_id
  WHERE
    t.created_at IS NOT NULL
    AND (
      su.signup_at IS NULL OR ru.signup_at IS NULL
      OR datetime(t.created_at) < datetime(su.signup_at)
      OR datetime(t.created_at) < datetime(ru.signup_at)
    )
),
orphan_events AS (
  SELECT e.event_id
  FROM app_events e
  LEFT JOIN users u ON u.user_id = e.user_id
  WHERE e.user_id IS NOT NULL AND u.user_id IS NULL
),
out_of_window_events AS (
  SELECT e.event_id
  FROM app_events e
  CROSS JOIN bounds b
  WHERE
    e.event_ts IS NOT NULL
    AND (
      datetime(e.event_ts) < datetime(b.window_start)
      OR datetime(e.event_ts) >= datetime(b.window_end)
    )
),
results AS (
  SELECT 1 AS ord, 'users_missing_email' AS anomaly,
         COUNT(*) AS bad_rows, NULL AS distinct_ids
  FROM users
  WHERE email IS NULL

  UNION ALL
  SELECT 2 AS ord, 'users_missing_signup_at' AS anomaly,
         COUNT(*) AS bad_rows, NULL AS distinct_ids
  FROM users
  WHERE signup_at IS NULL

  UNION ALL
  SELECT 3 AS ord, 'transactions_before_signup_or_unknown_signup' AS anomaly,
         (SELECT COUNT(*) FROM txn_before_signup) AS bad_rows, NULL AS distinct_ids

  UNION ALL
  SELECT 4 AS ord, 'transactions_duplicate_transaction_id' AS anomaly,
         (SELECT COALESCE(SUM(cnt), 0) FROM dup_txn) AS bad_rows,
         (SELECT COUNT(*) FROM dup_txn) AS distinct_ids

  UNION ALL
  SELECT 5 AS ord, 'transactions_missing_amount' AS anomaly,
         COUNT(*) AS bad_rows, NULL AS distinct_ids
  FROM transactions
  WHERE amount IS NULL

  UNION ALL
  SELECT 6 AS ord, 'app_events_orphan_user_id' AS anomaly,
         (SELECT COUNT(*) FROM orphan_events) AS bad_rows, NULL AS distinct_ids

  UNION ALL
  SELECT 7 AS ord, 'app_events_missing_event_type' AS anomaly,
         COUNT(*) AS bad_rows, NULL AS distinct_ids
  FROM app_events
  WHERE event_type IS NULL

  UNION ALL
  SELECT 8 AS ord, 'app_events_out_of_intended_month_window' AS anomaly,
         (SELECT COUNT(*) FROM out_of_window_events) AS bad_rows, NULL AS distinct_ids
)
SELECT anomaly, bad_rows, distinct_ids
FROM results
ORDER BY ord;

-- Note: distinct_ids is only populated for anomalies where counting unique identifiers is meaningful (duplicated transaction_id); otherwise it is left NULL.
