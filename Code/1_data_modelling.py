"""
Xapo Bank Take-Home Assessment (Data Analyst) — Part 1: Simulation (Python & Data Modelling)

Author: Afonso Carvalho, Date:2026-01-31

Goal
----
Generate 1 month of realistic synthetic data for a newly launched P2P payments feature.
The output is intentionally *not clean*: it includes noise, operational errors and edge cases
that frequently appear in production analytics pipelines.

Output files (relational model)
-------------------------------
1) users.csv
   - One row per user (dimension table).
   - Primary key: user_id
   - Used to validate lifecycle constraints.

2) transactions.csv
   - One row per P2P transfer attempt (fact table).
   - Foreign keys: sender_user_id, receiver_user_id → users.user_id
   - Includes transaction_id, created_at, amount, status, currency.
   - Used to test joins, deduplication logic, and temporal integrity checks.

3) app_events.csv
   - One row per user event in the app (logins, button clicks, page views).
   - Foreign key (expected): user_id → users.user_id
   - Used to test session, funnel metrics, attribution (events → transactions),
     and robustness to defective events.

Time window
-----------
Data is generated within a month, I assumed January 2026 for simplicity (as I expect to present this only in February), but some records
are intentionally out-of-window to simulate client clock drift, delayed ingestion, and replay.

Anomaly injection
------------------------------------------------
As asked I injected multiple anomaly classes to mirror real production issues:

A) Temporal inconsistencies (business rule violations)
   - Some transactions have created_at < signup_at for sender or receiver.
   - Real-world causes: delays in the user table updates, event-time vs ingestion-time confusion,
     backfilled history, timezone parsing mistakes, or user identity merges.

B) Duplicate transaction IDs (retry problems)
   - Some rows share the same transaction_id but differ in other fields.
   - Possible causes: mobile network retries, server timeouts...
   - Pipeline expectation: rather take (transaction_id, latest_created_at).

C) NULL / blank critical fields (schema drift, partial writes)
   - Missing amounts, missing event_type, missing signup_at / email, etc.
   - Real-world causes: client bugs, partial DB writes, privacy filters, evolving event schemas.
   - Pipeline expectation: enforce NOT NULL constraints where needed, impute/drop, alert, etc.

D) Orphan foreign keys (referential integrity failures)
   - Some app_events reference user_ids that do not exist in users.csv.
   - Real-world causes: anonymization, deleted accounts, late-arriving dimensions.

E) Out-of-window timestamps (clock drift / late events)
   - Some events occur slightly before/after the target month.
   - Real-world causes: device clock skew, offline mode + delayed sync, ingestion delays.

Notes for reviewers
-------------------
- Rates for anomalies are parameterized and can be tuned.
- The data is deterministic given a seed to allow reproducible pipeline tests.
- Gen AI was use to comment code and help debugging.
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from calendar import monthrange
from pathlib import Path

# ----------------------------
# Helpers Functions
# ----------------------------
def parse_month(month_str: str):
    # month_str: "YYYY-MM"
    year_s, month_s = month_str.split("-")
    year = int(year_s)
    month = int(month_s)
    start = datetime(year, month, 1, 0, 0, 0)
    days_in_month = monthrange(year, month)[1]
    end = start + timedelta(days=days_in_month)  # exclusive
    return start, end

def rand_dt(start: datetime, end: datetime) -> datetime:
    """Uniform random datetime in [start, end)."""
    delta = end - start
    seconds = int(delta.total_seconds())
    if seconds <= 0:
        return start
    return start + timedelta(seconds=random.randrange(seconds))

def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def write_csv(path: str, fieldnames, rows):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

# ----------------------------
# Data generation
# ----------------------------
FIRST_NAMES = [
    "Alex","Sam","Jordan","Taylor","Casey","Jamie","Riley","Morgan","Avery","Cameron",
    "Charlie","Drew","Emerson","Finley","Hayden","Jules","Kai","Logan","Noah","Quinn",
]
LAST_NAMES = [
    "Martin","Bernard","Thomas","Petit","Robert","Richard","Durand","Dubois","Moreau","Laurent",
    "Simon","Michel","Lefevre","Garcia","David","Bertrand","Roux","Vincent","Fournier","Morel",
]
COUNTRIES = ["FR","PT","ES","DE","IT","NL","BE","GB","IE","CH"]
CURRENCIES = ["BTC", "EUR", "USD"]

EVENT_TYPES = ["login", "page_view", "button_click", "logout"]
PAGES = ["/home", "/wallet", "/send", "/receive", "/settings", "/help", "/profile"]
BUTTONS = ["send_now", "request_money", "add_card", "logout", "support_chat", "confirm", "cancel"]
DEVICES = ["android", "ios", "web"]
OS_LIST = ["Android 14", "Android 13", "iOS 26", "iOS 18", "Windows 11", "macOS 14", "Ubuntu 22.04"]

def generate_users(n_users: int, month_start: datetime, month_end: datetime,
                   null_email_rate=0.01, null_signup_rate=0.01): # just a small fraction of users with missing email or signup_at
    users = []
    user_meta = {}  # user_id -> dict with signup_dt (may be None)
    for user_id in range(1, n_users + 1):
        fn = random.choice(FIRST_NAMES)
        ln = random.choice(LAST_NAMES)
        base_email = f"{fn}.{ln}{random.randint(10,9999)}@example.com".lower()

        # Most signups occur in the target month
        signup_dt = rand_dt(month_start, month_end)

        #Let's inject some anomalies here
        # Anomaly: missing signup_at (critical field)
        if random.random() < null_signup_rate:
            signup_dt_val = ""
            signup_dt_obj = None
        else:
            signup_dt_val = iso(signup_dt)
            signup_dt_obj = signup_dt
        # Anomaly: missing email (critical-ish field)
        email_val = "" if random.random() < null_email_rate else base_email
        # Small realism: created_at could match signup_at; keep it same here
        country = random.choice(COUNTRIES)

        users.append({
            "user_id": user_id,
            "first_name": fn,
            "last_name": ln,
            "email": email_val,
            "country": country,
            "signup_at": signup_dt_val,
        })
        user_meta[user_id] = {"signup_dt": signup_dt_obj}
    return users, user_meta

def generate_transactions(n_txns: int, n_users: int, month_start: datetime, month_end: datetime, user_meta,
                          before_signup_rate=0.02, dup_id_rate=0.01, null_amount_rate=0.01):
    # small fraction of transactions before signup, duplicate IDs, missing amounts
    txns = []

    def pick_distinct_users():
        sender = random.randint(1, n_users)
        receiver = random.randint(1, n_users)
        while receiver == sender:
            receiver = random.randint(1, n_users)
        return sender, receiver

    for _ in range(n_txns):
        sender_id, receiver_id = pick_distinct_users()
        sender_signup = user_meta[sender_id]["signup_dt"]
        receiver_signup = user_meta[receiver_id]["signup_dt"]

        # Create a "valid" timestamp after both signups when possible
        # If signup is missing, treat as unknown and just use month_start
        start_dt = month_start
        if sender_signup is not None:
            start_dt = max(start_dt, sender_signup)
        if receiver_signup is not None:
            start_dt = max(start_dt, receiver_signup)

        # Normally: created_at within [start_dt, month_end)
        created_at = rand_dt(start_dt, month_end)

        # Amount distribution: lognormal-ish via exponentiation
        amount = round((10 ** random.uniform(0.3, 2.2)), 2)  # ~2 to ~160
        currency = random.choices(CURRENCIES, weights=[0.85, 0.10, 0.05], k=1)[0]
        #As Xapo is a leader in crypto banks I assumed BTC is the most used currency

        status = random.choices(["completed", "pending", "failed"], weights=[0.90, 0.07, 0.03], k=1)[0]

        txns.append({
            "transaction_id": str(uuid.uuid4()),
            "sender_user_id": sender_id,
            "receiver_user_id": receiver_id,
            "amount": f"{amount:.2f}",
            "currency": currency,
            "status": status,
            "created_at": iso(created_at),
        })

    # ----------------------------
    # Inject anomalies in transactions, I suspect here is where most of the interest is
    # ----------------------------

    # 1) Temporal inconsistency: transaction before signup (for sender or receiver)
    n_before = max(1, int(n_txns * before_signup_rate))
    # choose users with later signup so we can create an earlier txn
    eligible_users = [
        uid for uid in range(1, n_users + 1)
        if user_meta[uid]["signup_dt"] is not None and user_meta[uid]["signup_dt"] > (month_start + timedelta(days=3))
    ]
    for i in random.sample(range(n_txns), k=min(n_before, n_txns)):
        if eligible_users:
            bad_user = random.choice(eligible_users)
            # Make this bad_user either sender or receiver
            if random.random() < 0.5:
                txns[i]["sender_user_id"] = bad_user
            else:
                txns[i]["receiver_user_id"] = bad_user

            # Force created_at to be before that user's signup
            bad_signup = user_meta[bad_user]["signup_dt"]
            forced_end = max(month_start + timedelta(hours=1), bad_signup - timedelta(minutes=1))
            forced_start = month_start
            txns[i]["created_at"] = iso(rand_dt(forced_start, forced_end))

    # 2) NULL in critical field: amount missing
    n_null_amount = max(1, int(n_txns * null_amount_rate))
    for i in random.sample(range(n_txns), k=min(n_null_amount, n_txns)):
        txns[i]["amount"] = ""  # NULL/blank critical field

    # 3) Duplicate transaction IDs: simulate retry logic
    n_dupes = max(1, int(n_txns * dup_id_rate))
    if n_txns > 2:
        # pick some rows to become duplicates of earlier ids
        dup_targets = random.sample(range(1, n_txns), k=min(n_dupes, n_txns - 1))
        for idx in dup_targets:
            source_idx = random.randrange(0, idx)  # earlier row
            txns[idx]["transaction_id"] = txns[source_idx]["transaction_id"]

    return txns

def generate_app_events(n_events: int, n_users: int, month_start: datetime, month_end: datetime,
                        orphan_user_rate=0.01, null_event_type_rate=0.005, out_of_window_rate=0.01):
    events = []

    def make_ip(): # simple random IPv4
        return f"{random.randint(1, 255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"

    for _ in range(n_events):
        user_id = random.randint(1, n_users)
        event_type = random.choice(EVENT_TYPES)
        ts = rand_dt(month_start, month_end)
        session_id = str(uuid.uuid4())
        device = random.choice(DEVICES)
        os_name = random.choice(OS_LIST)
        page = random.choice(PAGES)

        button_id = ""
        if event_type == "button_click":
            button_id = random.choice(BUTTONS)

        events.append({
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "event_type": event_type,
            "event_ts": iso(ts),
            "session_id": session_id,
            "page": page,
            "button_id": button_id,
            "device": device,
            "os": os_name,
            "ip": make_ip(),
        })

    # ----------------------------
    # Inject anomalies in app events
    # ----------------------------
    # 4) Orphan foreign keys: user_id does not exist
    n_orphan = max(1, int(n_events * orphan_user_rate))
    for i in random.sample(range(n_events), k=min(n_orphan, n_events)):
        events[i]["user_id"] = n_users + random.randint(1, 50)  # non-existent user ids

    # 5) NULL critical field: event_type missing
    n_null_type = max(1, int(n_events * null_event_type_rate))
    for i in random.sample(range(n_events), k=min(n_null_type, n_events)):
        events[i]["event_type"] = ""

    # 6) Out-of-window timestamps (before month or after month)
    n_oow = max(1, int(n_events * out_of_window_rate))
    for i in random.sample(range(n_events), k=min(n_oow, n_events)):
        if random.random() < 0.5:
            # before the month
            ts = rand_dt(month_start - timedelta(days=5), month_start)
        else:
            # after the month
            ts = rand_dt(month_end, month_end + timedelta(days=5))
        events[i]["event_ts"] = iso(ts)

    return events


from collections import Counter

#now in order to be able to replicate and verify the anomalies we need to summarize them
DT_FMT = "%Y-%m-%d %H:%M:%S"
def _is_null(v) -> bool:
    return v is None or str(v).strip() == ""

def _parse_dt(s: str):
    if _is_null(s):
        return None
    try:
        return datetime.strptime(str(s), DT_FMT)
    except ValueError:
        return None
    
def summarize_anomalies(users, txns, events, user_meta, month_start, month_end, n_users_expected: int):
    """
    Compute *observed* anomaly counts from in-memory data.
    This avoids vague 'subset' wording and makes the generator verifiable.
    """

    # Users
    users_missing_email = sum(1 for u in users if _is_null(u.get("email")))
    users_missing_signup = sum(1 for u in users if _is_null(u.get("signup_at")))

    # Transactions
    txn_missing_amount = sum(1 for t in txns if _is_null(t.get("amount")))

    txn_ids = [t.get("transaction_id") for t in txns if not _is_null(t.get("transaction_id"))]
    c = Counter(txn_ids)
    dup_ids = [tid for tid, cnt in c.items() if cnt > 1]
    txn_dup_distinct_ids = len(dup_ids)
    txn_dup_rows_total = sum(c[tid] for tid in dup_ids)     # all rows involved in duplicated IDs
    txn_dup_extra_rows = sum(c[tid] - 1 for tid in dup_ids) # duplicates beyond the first occurrence

    # created_at before signup OR unknown signup
    txn_before_signup_or_unknown = 0
    for t in txns:
        created = _parse_dt(t.get("created_at"))
        if created is None:
            continue

        sender_id = t.get("sender_user_id")
        receiver_id = t.get("receiver_user_id")

        # user_meta stores datetime objects for signup (or None)
        sender_signup = user_meta.get(sender_id, {}).get("signup_dt")
        receiver_signup = user_meta.get(receiver_id, {}).get("signup_dt")

        # Treat unknown signup as anomalous for lifecycle validation
        if sender_signup is None or receiver_signup is None:
            txn_before_signup_or_unknown += 1
            continue

        if created < sender_signup or created < receiver_signup:
            txn_before_signup_or_unknown += 1

    # Events
    events_missing_type = sum(1 for e in events if _is_null(e.get("event_type")))

    valid_users = set(range(1, n_users_expected + 1))
    events_orphan_user = 0
    for e in events:
        uid = e.get("user_id")
        if uid is None:
            continue
        try:
            uid_int = int(uid)
        except (TypeError, ValueError):
            events_orphan_user += 1
            continue
        if uid_int not in valid_users:
            events_orphan_user += 1

    events_out_of_window = 0
    for e in events:
        ts = _parse_dt(e.get("event_ts"))
        if ts is None:
            continue
        if ts < month_start or ts >= month_end:
            events_out_of_window += 1

    return {
        "users_missing_email": users_missing_email,
        "users_missing_signup_at": users_missing_signup,
        "transactions_missing_amount": txn_missing_amount,
        "transactions_duplicate_distinct_ids": txn_dup_distinct_ids,
        "transactions_duplicate_rows_total": txn_dup_rows_total,
        "transactions_duplicate_extra_rows": txn_dup_extra_rows,
        "transactions_before_signup_or_unknown_signup": txn_before_signup_or_unknown,
        "events_missing_event_type": events_missing_type,
        "events_orphan_user_id": events_orphan_user,
        "events_out_of_window": events_out_of_window,
    }

# ----------------------------
# Main
# ----------------------------
def main():
    ap = argparse.ArgumentParser()

    # Output
    ap.add_argument("--outdir", default=None, help="Output directory for CSVs. Default: <project_root>/Data")

    # Repro / size
    ap.add_argument("--month", default="2026-01", help='Target month "YYYY-MM"')
    ap.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    ap.add_argument("--n_users", type=int, default=1000)
    ap.add_argument("--n_txns", type=int, default=5000)
    ap.add_argument("--n_events", type=int, default=10000)

    # Anomaly rates (easy knobs)
    ap.add_argument("--null-email-rate", type=float, default=0.01)
    ap.add_argument("--null-signup-rate", type=float, default=0.01)

    ap.add_argument("--before-signup-rate", type=float, default=0.02)
    ap.add_argument("--dup-id-rate", type=float, default=0.01)
    ap.add_argument("--null-amount-rate", type=float, default=0.01)

    ap.add_argument("--orphan-user-rate", type=float, default=0.01)
    ap.add_argument("--null-event-type-rate", type=float, default=0.005)
    ap.add_argument("--out-of-window-rate", type=float, default=0.01)

    args = ap.parse_args()

    random.seed(args.seed)
    month_start, month_end = parse_month(args.month)

    # --- Generate data ---
    users, user_meta = generate_users(
        n_users=args.n_users,
        month_start=month_start,
        month_end=month_end,
        null_email_rate=args.null_email_rate,
        null_signup_rate=args.null_signup_rate,
    )

    txns = generate_transactions(
        n_txns=args.n_txns,
        n_users=args.n_users,
        month_start=month_start,
        month_end=month_end,
        user_meta=user_meta,
        before_signup_rate=args.before_signup_rate,
        dup_id_rate=args.dup_id_rate,
        null_amount_rate=args.null_amount_rate,
    )

    events = generate_app_events(
        n_events=args.n_events,
        n_users=args.n_users,
        month_start=month_start,
        month_end=month_end,
        orphan_user_rate=args.orphan_user_rate,
        null_event_type_rate=args.null_event_type_rate,
        out_of_window_rate=args.out_of_window_rate,
    )

    # --- Output paths (stable: always <project_root>/Data by default) ---
    project_root = Path(__file__).resolve().parent.parent
    outdir = Path(args.outdir) if args.outdir else (project_root / "Data")
    outdir.mkdir(parents=True, exist_ok=True)

    users_path = outdir / "users.csv"
    txns_path = outdir / "transactions.csv"
    events_path = outdir / "app_events.csv"

    write_csv(users_path,
              ["user_id", "first_name", "last_name", "email", "country", "signup_at"],
              users)
    write_csv(txns_path,
              ["transaction_id", "sender_user_id", "receiver_user_id", "amount", "currency", "status", "created_at"],
              txns)
    write_csv(events_path,
              ["event_id", "user_id", "event_type", "event_ts", "session_id", "page", "button_id", "device", "os", "ip"],
              events)

    # --- Targets (what we *intended* to inject) ---
    targets = {
        "users_missing_email": max(1, int(args.n_users * args.null_email_rate)),
        "users_missing_signup_at": max(1, int(args.n_users * args.null_signup_rate)),
        "transactions_before_signup_or_unknown_signup": max(1, int(args.n_txns * args.before_signup_rate)),
        "transactions_duplicate_rows_target": max(1, int(args.n_txns * args.dup_id_rate)),
        "transactions_missing_amount": max(1, int(args.n_txns * args.null_amount_rate)),
        "events_orphan_user_id": max(1, int(args.n_events * args.orphan_user_rate)),
        "events_missing_event_type": max(1, int(args.n_events * args.null_event_type_rate)),
        "events_out_of_window": max(1, int(args.n_events * args.out_of_window_rate)),
    }

    # --- Observed (what actually ended up in the data) ---
    observed = summarize_anomalies(users, txns, events, user_meta, month_start, month_end, args.n_users)

    # --- Print summary ---
    print("Generated:")
    print(f" - {users_path} ({len(users)} rows)")
    print(f" - {txns_path} ({len(txns)} rows)")
    print(f" - {events_path} ({len(events)} rows)")
    print(f"\nConfig: month={args.month}, seed={args.seed}, users={args.n_users}, txns={args.n_txns}, events={args.n_events}")

    print("\nIntentional anomalies (target -> observed):")
    print(f" - users missing email: {targets['users_missing_email']} -> {observed['users_missing_email']}")
    print(f" - users missing signup_at: {targets['users_missing_signup_at']} -> {observed['users_missing_signup_at']}")
    print(f" - tx before signup/unknown signup: {targets['transactions_before_signup_or_unknown_signup']} -> {observed['transactions_before_signup_or_unknown_signup']}")
    print(f" - tx duplicate IDs (rows targeted): {targets['transactions_duplicate_rows_target']} -> "
          f"{observed['transactions_duplicate_rows_total']} rows across {observed['transactions_duplicate_distinct_ids']} IDs "
          f"(extra rows={observed['transactions_duplicate_extra_rows']})")
    print(f" - tx missing amount: {targets['transactions_missing_amount']} -> {observed['transactions_missing_amount']}")
    print(f" - events orphan user_id: {targets['events_orphan_user_id']} -> {observed['events_orphan_user_id']}")
    print(f" - events missing event_type: {targets['events_missing_event_type']} -> {observed['events_missing_event_type']}")
    print(f" - events out-of-window: {targets['events_out_of_window']} -> {observed['events_out_of_window']}")

if __name__ == "__main__":
    main()
