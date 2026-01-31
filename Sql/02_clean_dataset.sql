-- Solve anomalies by creating clean views.
-- These are conservative “trusted” rules for analytics.

DROP VIEW IF EXISTS clean_users;
DROP VIEW IF EXISTS clean_transactions;
DROP VIEW IF EXISTS clean_app_events;

-- Clean users:
-- Keep users, but require a known signup_at because it's needed for lifecycle validation.
-- (Email is not required for the metrics in this take-home.)
CREATE VIEW clean_users AS
SELECT *
FROM users
WHERE signup_at IS NOT NULL
;

-- Clean transactions:
-- Rules:
-- 1) Drop rows missing critical fields (transaction_id, created_at, amount, sender/receiver).
-- 2) Require sender/receiver to exist in clean_users (and thus have signup_at).
-- 3) Enforce lifecycle: created_at >= sender.signup_at AND >= receiver.signup_at
-- 4) Deduplicate transaction_id (keep “best” row):
--    - Prefer completed > pending > failed
--    - Then most recent created_at
CREATE VIEW clean_transactions AS
WITH
tx_base AS (
  SELECT
    t.*,
    CASE t.status
      WHEN 'completed' THEN 3
      WHEN 'pending'   THEN 2
      WHEN 'failed'    THEN 1
      ELSE 0
    END AS status_rank
  FROM transactions t
  WHERE
    t.transaction_id IS NOT NULL
    AND t.created_at IS NOT NULL
    AND t.amount IS NOT NULL
    AND t.sender_user_id IS NOT NULL
    AND t.receiver_user_id IS NOT NULL
),
tx_valid_fk AS (
  SELECT b.*
  FROM tx_base b
  JOIN clean_users su ON su.user_id = b.sender_user_id
  JOIN clean_users ru ON ru.user_id = b.receiver_user_id
  WHERE
    datetime(b.created_at) >= datetime(su.signup_at)
    AND datetime(b.created_at) >= datetime(ru.signup_at)
),
tx_ranked AS (
  SELECT
    v.*,
    ROW_NUMBER() OVER (
      PARTITION BY v.transaction_id
      ORDER BY v.status_rank DESC, datetime(v.created_at) DESC
    ) AS rn
  FROM tx_valid_fk v
)
SELECT
  transaction_id,
  sender_user_id,
  receiver_user_id,
  amount,
  currency,
  status,
  created_at
FROM tx_ranked
WHERE rn = 1
;

-- Clean app events:
-- Infer intended window from transactions (events include deliberate out-of-window timestamps).
-- Rules:
-- 1) Drop missing critical fields: event_ts, event_type, user_id
-- 2) Drop orphan user_ids (must exist in clean_users)
-- 3) Keep only events within intended month window
CREATE VIEW clean_app_events AS
WITH
bounds AS (
  SELECT
    date(min(created_at), 'start of month') AS window_start,
    date(min(created_at), 'start of month', '+1 month') AS window_end
  FROM transactions
  WHERE created_at IS NOT NULL
)
SELECT
  e.*
FROM app_events e
JOIN clean_users u ON u.user_id = e.user_id
CROSS JOIN bounds b
WHERE
  e.user_id IS NOT NULL
  AND e.event_ts IS NOT NULL
  AND e.event_type IS NOT NULL
  AND datetime(e.event_ts) >= datetime(b.window_start)
  AND datetime(e.event_ts) <  datetime(b.window_end)
;
