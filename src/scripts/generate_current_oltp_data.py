from __future__ import annotations

import argparse
from datetime import date, timedelta

import pandas as pd
import psycopg
from load_oltp_seed import (
    CUSTOMER_COLUMNS,
    FRAUD_CASE_COLUMNS,
    SCENARIO_DELAY_DAYS,
    SCENARIO_RESOLUTION_SOURCE,
    TERMINAL_COLUMNS,
    TRANSACTION_COLUMNS,
    build_accounts,
    build_cards,
    build_customers,
    build_terminals,
    collect_unique_ids,
    copy_dataframe,
    list_seed_files,
    read_seed_frame,
    truncate_tables,
    validate_loaded_counts,
)


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Expected date in YYYY-MM-DD format, got {value!r}"
        ) from None


def resolve_date_range(
    start_date: date | None,
    end_date: date | None,
    days: int | None,
) -> tuple[date, date]:
    if days is not None and days <= 0:
        raise ValueError("--days must be a positive integer")

    resolved_end = end_date or date.today()
    if start_date is None:
        window_days = days or 30
        start_date = resolved_end - timedelta(days=window_days - 1)
    elif days is not None and end_date is None:
        resolved_end = start_date + timedelta(days=days - 1)

    if start_date > resolved_end:
        raise ValueError("--start-date must be on or before --end-date")

    return start_date, resolved_end


def fetch_latest_transaction_date(connection: psycopg.Connection) -> date | None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT MAX(event_timestamp) FROM banking.transactions")
        latest_timestamp = cursor.fetchone()[0]

    if latest_timestamp is None:
        return None
    if isinstance(latest_timestamp, date) and not isinstance(latest_timestamp, pd.Timestamp):
        return latest_timestamp.date() if hasattr(latest_timestamp, "date") else latest_timestamp
    return pd.Timestamp(latest_timestamp).date()


def resolve_generation_range(
    latest_date: date | None,
    start_date: date | None,
    end_date: date | None,
    days: int | None,
) -> tuple[date, date] | None:
    if start_date is not None:
        if latest_date is not None and start_date <= latest_date:
            raise ValueError(
                f"--start-date {start_date} overlaps existing OLTP data through "
                f"{latest_date}"
            )
        return resolve_date_range(start_date, end_date, days)

    if latest_date is None:
        return resolve_date_range(None, end_date, days)

    resume_date = latest_date + timedelta(days=1)
    resolved_end = end_date or date.today()
    if days is not None:
        if days <= 0:
            raise ValueError("--days must be a positive integer")
        if end_date is None:
            resolved_end = resume_date + timedelta(days=days - 1)

    if resume_date > resolved_end:
        return None
    return resume_date, resolved_end


def iter_dates(start_date: date, end_date: date) -> list[date]:
    return [
        start_date + timedelta(days=offset)
        for offset in range((end_date - start_date).days + 1)
    ]


def sample_daily_frame(
    frame: pd.DataFrame,
    daily_scale: float,
    seed: int,
) -> pd.DataFrame:
    if daily_scale <= 0:
        raise ValueError("--daily-scale must be greater than 0")

    if daily_scale >= 1:
        return frame.copy()

    sample_size = max(1, int(round(len(frame) * daily_scale)))
    return frame.sample(n=sample_size, random_state=seed).sort_values("TX_DATETIME")


def shift_frame_to_target_date(
    frame: pd.DataFrame,
    target_date: date,
    feature_day_index: int,
    next_transaction_id: int,
) -> pd.DataFrame:
    shifted = frame.copy()
    timestamps = pd.to_datetime(shifted["TX_DATETIME"])
    target_midnight = pd.Timestamp(target_date)
    seconds_in_day = (
        timestamps.dt.hour * 3600
        + timestamps.dt.minute * 60
        + timestamps.dt.second
    )

    shifted["TX_DATETIME"] = target_midnight + pd.to_timedelta(seconds_in_day, unit="s")
    shifted["TX_TIME_SECONDS"] = seconds_in_day.astype(int)
    shifted["TX_TIME_DAYS"] = int(feature_day_index)
    shifted["TRANSACTION_ID"] = range(next_transaction_id, next_transaction_id + len(shifted))
    return shifted


def build_current_transactions(
    frame: pd.DataFrame,
    account_lookup: dict[str, str],
    card_lookup: dict[str, str],
) -> pd.DataFrame:
    transactions = pd.DataFrame(
        {
            "transaction_id": frame["TRANSACTION_ID"].astype("int64"),
            "event_timestamp": frame["TX_DATETIME"],
            "customer_id": frame["CUSTOMER_ID"].astype(str),
            "account_id": frame["CUSTOMER_ID"].astype(str).map(account_lookup),
            "card_id": frame["CUSTOMER_ID"].astype(str).map(card_lookup),
            "terminal_id": frame["TERMINAL_ID"].astype(str),
            "amount": frame["TX_AMOUNT"].astype(float).round(2),
            "currency_code": "EUR",
            "transaction_type": "purchase",
            "channel_type": "pos",
            "auth_status": "approved",
            "tx_time_seconds": frame["TX_TIME_SECONDS"].astype(int),
            "tx_time_days": frame["TX_TIME_DAYS"].astype(int),
            "is_weekend": frame["TX_DATETIME"].dt.dayofweek >= 5,
            "is_night": (frame["TX_DATETIME"].dt.hour < 6) | (frame["TX_DATETIME"].dt.hour >= 22),
            "created_at": frame["TX_DATETIME"],
        }
    )
    return transactions.loc[:, TRANSACTION_COLUMNS]


def build_current_fraud_cases(
    frame: pd.DataFrame,
    transactions: pd.DataFrame,
    visible_until: date,
) -> pd.DataFrame:
    fraud_frame = frame.loc[frame["TX_FRAUD"].astype(int) == 1].copy()
    if fraud_frame.empty:
        return pd.DataFrame(columns=FRAUD_CASE_COLUMNS)

    tx_lookup = transactions.set_index("transaction_id")
    rows = []
    visible_until_ts = pd.Timestamp(visible_until) + pd.Timedelta(days=1)

    for fraud_row in fraud_frame.itertuples(index=False):
        transaction_id = int(fraud_row.TRANSACTION_ID)
        transaction = tx_lookup.loc[transaction_id]
        scenario = int(fraud_row.TX_FRAUD_SCENARIO)
        delay_days = SCENARIO_DELAY_DAYS.get(scenario, 14)
        reported_at = pd.Timestamp(transaction["event_timestamp"]) + pd.Timedelta(days=delay_days)

        if reported_at >= visible_until_ts:
            continue

        planned_resolved_at = reported_at + pd.Timedelta(days=max(1, scenario))
        if planned_resolved_at >= visible_until_ts:
            case_status = "open"
            resolved_at = None
        else:
            case_status = "confirmed_fraud"
            resolved_at = planned_resolved_at

        rows.append(
            {
                "case_id": f"CASE-{transaction_id}",
                "transaction_id": transaction_id,
                "customer_id": str(fraud_row.CUSTOMER_ID),
                "card_id": transaction["card_id"],
                "fraud_scenario": scenario,
                "case_status": case_status,
                "resolution_source": SCENARIO_RESOLUTION_SOURCE.get(scenario, "manual_review"),
                "reported_at": reported_at,
                "resolved_at": resolved_at,
                "loss_amount": round(float(transaction["amount"]), 2),
                "created_at": reported_at,
            }
        )

    return pd.DataFrame(rows, columns=FRAUD_CASE_COLUMNS)


def fetch_next_transaction_id(connection: psycopg.Connection) -> int:
    with connection.cursor() as cursor:
        cursor.execute("SELECT COALESCE(MAX(transaction_id), 0) + 1 FROM banking.transactions")
        return int(cursor.fetchone()[0])


def table_count(connection: psycopg.Connection, table_name: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM banking.{table_name}")
        return int(cursor.fetchone()[0])


def print_timestamp_summary(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*), MIN(event_timestamp), MAX(event_timestamp)
            FROM banking.transactions
            """
        )
        tx_count, min_ts, max_ts = cursor.fetchone()
        cursor.execute(
            """
            SELECT
                COUNT(*),
                MIN(reported_at),
                MAX(reported_at),
                COUNT(*) FILTER (WHERE resolved_at IS NULL)
            FROM banking.fraud_cases
            """
        )
        case_count, min_reported, max_reported, open_cases = cursor.fetchone()

    print("\nTimestamp summary:")
    print(f"- transactions: {tx_count:,} rows, {min_ts} -> {max_ts}")
    print(
        f"- fraud_cases: {case_count:,} rows, {min_reported} -> {max_reported}, "
        f"open={open_cases:,}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate current-date OLTP demo data from historical fraud seed files."
    )
    parser.add_argument("--data-dir", default="data", help="Directory containing .pkl seed files")
    parser.add_argument("--pattern", default="*.pkl", help="Seed file glob pattern")
    parser.add_argument("--db-host", default="localhost", help="Postgres host")
    parser.add_argument("--db-port", type=int, default=5433, help="Postgres port")
    parser.add_argument("--db-name", default="fraud_bank", help="Postgres database name")
    parser.add_argument("--db-user", default="postgres", help="Postgres user")
    parser.add_argument("--db-password", default="postgres", help="Postgres password")
    parser.add_argument("--start-date", type=parse_date, default=None, help="First generated date, YYYY-MM-DD")
    parser.add_argument("--end-date", type=parse_date, default=None, help="Last generated date, YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=None, help="Generated day count when start/end are omitted")
    parser.add_argument("--daily-scale", type=float, default=0.10, help="Fraction of each seed day to load")
    parser.add_argument("--max-days", type=int, default=None, help="Optional cap on generated days for quick smoke tests")
    parser.add_argument("--reset", action="store_true", help="Truncate OLTP tables before loading")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    connection = psycopg.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    try:
        if args.reset:
            print("Truncating existing OLTP tables...")
            truncate_tables(connection)
            connection.commit()

        latest_date = fetch_latest_transaction_date(connection)
        generation_range = resolve_generation_range(
            latest_date,
            args.start_date,
            args.end_date,
            args.days,
        )
        if generation_range is None:
            print(
                "OLTP data is already current through "
                f"{latest_date}; no rows were inserted."
            )
            return

        start_date, end_date = generation_range
        target_dates = iter_dates(start_date, end_date)
        if args.max_days is not None:
            if args.max_days <= 0:
                raise ValueError("--max-days must be a positive integer")
            target_dates = target_dates[: args.max_days]

        seed_files = list_seed_files(args.data_dir, args.pattern, limit_files=None)
        print(f"Found {len(seed_files)} seed files")
        print(f"Generating {len(target_dates)} day(s): {target_dates[0]} -> {target_dates[-1]}")
        print(f"Daily scale: {args.daily_scale}")

        customer_ids, terminal_ids = collect_unique_ids(seed_files)
        customers = build_customers(customer_ids)
        accounts = build_accounts(customers)
        cards = build_cards(customers, accounts)
        terminals = build_terminals(terminal_ids)
        account_lookup = accounts.set_index("customer_id")["account_id"].to_dict()
        card_lookup = cards.set_index("customer_id")["card_id"].to_dict()

        should_load_dimensions = args.reset or table_count(connection, "customers") == 0
        if should_load_dimensions:
            print("Loading dimension tables...")
            copy_dataframe(connection, "customers", CUSTOMER_COLUMNS, customers)
            copy_dataframe(connection, "accounts", accounts.columns.tolist(), accounts)
            copy_dataframe(connection, "cards", cards.columns.tolist(), cards)
            copy_dataframe(connection, "terminals", TERMINAL_COLUMNS, terminals)
            connection.commit()

        next_transaction_id = fetch_next_transaction_id(connection)
        total_transactions = 0
        total_fraud_cases = 0

        for index, target_date in enumerate(target_dates):
            seed_file = seed_files[index % len(seed_files)]
            seed_frame = read_seed_frame(seed_file)
            daily_frame = sample_daily_frame(seed_frame, args.daily_scale, seed=index + 17)
            shifted_frame = shift_frame_to_target_date(
                daily_frame,
                target_date,
                feature_day_index=index,
                next_transaction_id=next_transaction_id,
            )
            transactions = build_current_transactions(shifted_frame, account_lookup, card_lookup)
            fraud_cases = build_current_fraud_cases(
                shifted_frame,
                transactions,
                visible_until=end_date,
            )

            copy_dataframe(connection, "transactions", TRANSACTION_COLUMNS, transactions)
            copy_dataframe(connection, "fraud_cases", FRAUD_CASE_COLUMNS, fraud_cases)
            connection.commit()

            next_transaction_id += len(transactions)
            total_transactions += len(transactions)
            total_fraud_cases += len(fraud_cases)
            print(
                f"[{target_date}] from {seed_file.name}: "
                f"{len(transactions):,} transactions, {len(fraud_cases):,} fraud cases"
            )

        counts = validate_loaded_counts(connection)
        print("\nFinal table counts:")
        for table_name, count in counts.items():
            print(f"- {table_name}: {count:,}")

        print(
            f"\nLoad complete: {total_transactions:,} transactions and "
            f"{total_fraud_cases:,} fraud cases inserted"
        )
        print_timestamp_summary(connection)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
