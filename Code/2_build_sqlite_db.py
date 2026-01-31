"""
Build a small SQLite database from the generated CSVs.
"""

import argparse
import csv
import os
import sqlite3


def empty_to_none(v: str):
    if v is None:
        return None
    v = v.strip()
    return None if v == "" else v


def to_int(v: str):
    v = empty_to_none(v)
    return None if v is None else int(v)


def to_float(v: str):
    v = empty_to_none(v)
    return None if v is None else float(v)


def read_csv(path: str):
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def create_schema(conn: sqlite3.Connection):
    cur = conn.cursor()

    # Drop existing tables for reproducibility
    cur.executescript(
        """
        PRAGMA foreign_keys = ON;

        DROP TABLE IF EXISTS app_events;
        DROP TABLE IF EXISTS transactions;
        DROP TABLE IF EXISTS users;

        -- Dimension table
        CREATE TABLE users (
            user_id       INTEGER PRIMARY KEY,
            first_name    TEXT,
            last_name     TEXT,
            email         TEXT,     -- can be NULL (intentional anomaly)
            country       TEXT,
            signup_at     TEXT      -- store as ISO string; can be NULL (intentional anomaly)
        );

        -- Fact table
        CREATE TABLE transactions (
            transaction_id   TEXT,  -- not enforcing PK because duplicates are an intentional anomaly
            sender_user_id   INTEGER,
            receiver_user_id INTEGER,
            amount           REAL,  -- can be NULL (intentional anomaly)
            currency         TEXT,
            status           TEXT,
            created_at       TEXT,
            FOREIGN KEY(sender_user_id) REFERENCES users(user_id),
            FOREIGN KEY(receiver_user_id) REFERENCES users(user_id)
        );

        -- Telemetry table
        CREATE TABLE app_events (
            event_id     TEXT PRIMARY KEY,
            user_id      INTEGER,  -- can reference missing users (intentional anomaly)
            event_type   TEXT,     -- can be NULL (intentional anomaly)
            event_ts     TEXT,
            session_id   TEXT,
            page         TEXT,
            button_id    TEXT,
            device       TEXT,
            os           TEXT,
            ip           TEXT
        );

        -- Helpful indexes for common analytics patterns
        CREATE INDEX idx_txn_created_at ON transactions(created_at);
        CREATE INDEX idx_txn_sender ON transactions(sender_user_id);
        CREATE INDEX idx_txn_receiver ON transactions(receiver_user_id);

        CREATE INDEX idx_evt_ts ON app_events(event_ts);
        CREATE INDEX idx_evt_user ON app_events(user_id);
        CREATE INDEX idx_evt_type ON app_events(event_type);
        """
    )
    conn.commit()


def load_users(conn: sqlite3.Connection, users_csv: str):
    rows = read_csv(users_csv)
    cur = conn.cursor()

    payload = []
    for r in rows:
        payload.append(
            (
                to_int(r["user_id"]),
                empty_to_none(r.get("first_name")),
                empty_to_none(r.get("last_name")),
                empty_to_none(r.get("email")),
                empty_to_none(r.get("country")),
                empty_to_none(r.get("signup_at")),
            )
        )

    cur.executemany(
        """
        INSERT INTO users(user_id, first_name, last_name, email, country, signup_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    conn.commit()


def load_transactions(conn: sqlite3.Connection, txns_csv: str):
    rows = read_csv(txns_csv)
    cur = conn.cursor()

    payload = []
    for r in rows:
        payload.append(
            (
                empty_to_none(r.get("transaction_id")),
                to_int(r.get("sender_user_id")),
                to_int(r.get("receiver_user_id")),
                to_float(r.get("amount")),
                empty_to_none(r.get("currency")),
                empty_to_none(r.get("status")),
                empty_to_none(r.get("created_at")),
            )
        )

    cur.executemany(
        """
        INSERT INTO transactions(transaction_id, sender_user_id, receiver_user_id, amount, currency, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    conn.commit()


def load_app_events(conn: sqlite3.Connection, events_csv: str):
    rows = read_csv(events_csv)
    cur = conn.cursor()

    payload = []
    for r in rows:
        payload.append(
            (
                empty_to_none(r.get("event_id")),
                to_int(r.get("user_id")),
                empty_to_none(r.get("event_type")),
                empty_to_none(r.get("event_ts")),
                empty_to_none(r.get("session_id")),
                empty_to_none(r.get("page")),
                empty_to_none(r.get("button_id")),
                empty_to_none(r.get("device")),
                empty_to_none(r.get("os")),
                empty_to_none(r.get("ip")),
            )
        )

    cur.executemany(
        """
        INSERT INTO app_events(event_id, user_id, event_type, event_ts, session_id, page, button_id, device, os, ip)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    conn.commit()


def print_counts(conn: sqlite3.Connection):
    cur = conn.cursor()
    for t in ["users", "transactions", "app_events"]:
        n = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"{t}: {n}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv-dir", default="./data", help="Folder containing the three CSV files")
    ap.add_argument("--db-path", default="./db/xapo_p2p.sqlite", help="Output SQLite DB path")
    args = ap.parse_args()

    users_csv = os.path.join(args.csv_dir, "users.csv")
    txns_csv = os.path.join(args.csv_dir, "transactions.csv")
    events_csv = os.path.join(args.csv_dir, "app_events.csv")

    os.makedirs(os.path.dirname(args.db_path), exist_ok=True)

    conn = sqlite3.connect(args.db_path)
    try:
        create_schema(conn)
        load_users(conn, users_csv)
        load_transactions(conn, txns_csv)
        load_app_events(conn, events_csv)
        print(f"DB created at: {args.db_path}")
        print_counts(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
