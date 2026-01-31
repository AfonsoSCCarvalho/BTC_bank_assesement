# Xapo Take-Home — P2P Simulation + SQL - Afonso Carvalho

## What it does
- Generates 1 month of **imperfect** CSV data: `users.csv`, `transactions.csv`, `app_events.csv`
- Loads CSVs into a **SQLite** database: `db/xapo_p2p.sqlite`
- Runs SQL to **summarize anomalies**, **create clean views**, and **compute trusted metrics**

## Files & folders
- `Code/data_modelling.py` — generates CSVs into `Data/`
- `Code/build_sqlite_db.py` — creates `db/xapo_p2p.sqlite` from `Data/`
- `Sql/01_identify_anomalies.sql` — anomaly summary
- `Sql/02_clean_dataset.sql` — creates views: `clean_users`, `clean_transactions`, `clean_app_events`
- `Sql/03_metrics.sql` — metrics from clean views

## How to run
1. Run `Code/data_modelling.py` → CSVs saved in `Data/`
2. Run `Code/build_sqlite_db.py` → DB saved in `db/`
3. Execute the SQL scripts in `Sql/` in order: 01 → 02 → 03

## Intentional anomalies
- Transactions before signup
- Duplicate `transaction_id`
- NULL critical fields
- Orphan `user_id` in app events
- Out-of-window event timestamps

## Trusted metrics (from clean views)
- Total Volume Transacted
- Daily Active Users
- Average Transaction Size per User
