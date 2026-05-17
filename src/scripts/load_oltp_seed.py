from __future__ import annotations

import argparse
import hashlib
import io
from pathlib import Path

import pandas as pd
import psycopg

CUSTOMER_SEGMENTS = ["mass", "affluent", "small_business", "student"]
CUSTOMER_REGIONS = ["north", "south", "east", "west", "central"]
RISK_TIERS = ["low", "medium", "high"]
ACCOUNT_TYPES = ["checking", "salary", "digital_wallet"]
CARD_TYPES = ["debit", "credit", "prepaid"]
CARD_NETWORKS = ["visa", "mastercard", "amex"]
MERCHANT_CATEGORIES = [
    "grocery",
    "fuel",
    "restaurant",
    "electronics",
    "travel",
    "fashion",
    "utilities",
    "atm",
]
CHANNEL_TYPES = ["pos", "atm", "ecommerce"]
CITIES = ["hanoi", "hcmc", "da_nang", "can_tho", "hai_phong"]
COUNTRY_CODE = "VN"
TRANSACTION_TYPES = ["purchase", "cash_withdrawal", "bill_payment"]
SCENARIO_DELAY_DAYS = {1: 1, 2: 7, 3: 30}
SCENARIO_RESOLUTION_SOURCE = {1: "rule_engine", 2: "manual_review", 3: "chargeback"}

CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_segment",
    "customer_region",
    "country_code",
    "risk_tier",
    "kyc_status",
    "customer_status",
    "onboarding_date",
    "created_at",
    "updated_at",
]
ACCOUNT_COLUMNS = [
    "account_id",
    "customer_id",
    "account_type",
    "currency_code",
    "account_status",
    "available_balance",
    "opened_at",
    "created_at",
    "updated_at",
]
CARD_COLUMNS = [
    "card_id",
    "account_id",
    "customer_id",
    "card_type",
    "card_network",
    "card_status",
    "issued_at",
    "expiry_date",
    "created_at",
    "updated_at",
]
TERMINAL_COLUMNS = [
    "terminal_id",
    "merchant_id",
    "merchant_category",
    "channel_type",
    "city",
    "terminal_region",
    "country_code",
    "risk_band",
    "terminal_status",
    "installed_at",
    "created_at",
    "updated_at",
]
TRANSACTION_COLUMNS = [
    "transaction_id",
    "event_timestamp",
    "customer_id",
    "account_id",
    "card_id",
    "terminal_id",
    "amount",
    "currency_code",
    "transaction_type",
    "channel_type",
    "auth_status",
    "tx_time_seconds",
    "tx_time_days",
    "is_weekend",
    "is_night",
    "created_at",
]
FRAUD_CASE_COLUMNS = [
    "case_id",
    "transaction_id",
    "customer_id",
    "card_id",
    "fraud_scenario",
    "case_status",
    "resolution_source",
    "reported_at",
    "resolved_at",
    "loss_amount",
    "created_at",
]
LOAD_LOCK_ID = 20260423


def stable_int(value: str) -> int:
    digest = hashlib.sha256(str(value).encode("utf-8")).hexdigest()
    return int(digest[:16], 16)


def pick(value: str, choices: list[str]) -> str:
    return choices[stable_int(value) % len(choices)]


def normalize_id(series: pd.Series) -> pd.Series:
    return series.astype(str)


def list_seed_files(data_dir: str, pattern: str, limit_files: int | None) -> list[Path]:
    files = sorted(Path(data_dir).glob(pattern))
    if limit_files is not None:
        files = files[:limit_files]
    if not files:
        raise FileNotFoundError(f"No seed files found in {data_dir!r} with pattern {pattern!r}")
    return files


def read_seed_frame(file_path: Path) -> pd.DataFrame:
    frame = pd.read_pickle(file_path).copy()
    frame["CUSTOMER_ID"] = normalize_id(frame["CUSTOMER_ID"])
    frame["TERMINAL_ID"] = normalize_id(frame["TERMINAL_ID"])
    frame["TX_DATETIME"] = pd.to_datetime(frame["TX_DATETIME"])
    frame["TX_TIME_SECONDS"] = pd.to_numeric(frame["TX_TIME_SECONDS"], errors="coerce").fillna(0).astype(int)
    frame["TX_TIME_DAYS"] = pd.to_numeric(frame["TX_TIME_DAYS"], errors="coerce").fillna(0).astype(int)
    frame["TX_FRAUD"] = pd.to_numeric(frame["TX_FRAUD"], errors="coerce").fillna(0).astype(int)
    frame["TX_FRAUD_SCENARIO"] = pd.to_numeric(frame["TX_FRAUD_SCENARIO"], errors="coerce").fillna(0).astype(int)
    return frame


def collect_unique_ids(file_paths: list[Path]) -> tuple[list[str], list[str]]:
    customer_ids: set[str] = set()
    terminal_ids: set[str] = set()

    for file_path in file_paths:
        frame = read_seed_frame(file_path)
        customer_ids.update(frame["CUSTOMER_ID"].unique().tolist())
        terminal_ids.update(frame["TERMINAL_ID"].unique().tolist())

    return sorted(customer_ids), sorted(terminal_ids)


def build_customers(customer_ids: list[str]) -> pd.DataFrame:
    rows = []
    base_date = pd.Timestamp("2016-01-01")

    for customer_id in customer_ids:
        seed = stable_int(customer_id)
        onboarding_date = base_date + pd.Timedelta(days=seed % 720)
        rows.append(
            {
                "customer_id": customer_id,
                "customer_segment": pick(customer_id, CUSTOMER_SEGMENTS),
                "customer_region": pick(f"region-{customer_id}", CUSTOMER_REGIONS),
                "country_code": COUNTRY_CODE,
                "risk_tier": pick(f"risk-{customer_id}", RISK_TIERS),
                "kyc_status": "verified",
                "customer_status": "active",
                "onboarding_date": onboarding_date.date(),
                "created_at": onboarding_date,
                "updated_at": onboarding_date,
            }
        )

    return pd.DataFrame(rows, columns=CUSTOMER_COLUMNS)


def build_accounts(customers: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for customer in customers.itertuples(index=False):
        account_id = f"ACC-{int(customer.customer_id):06d}" if customer.customer_id.isdigit() else f"ACC-{customer.customer_id}"
        seed = stable_int(customer.customer_id)
        opened_at = pd.Timestamp(customer.created_at)
        rows.append(
            {
                "account_id": account_id,
                "customer_id": customer.customer_id,
                "account_type": pick(f"account-{customer.customer_id}", ACCOUNT_TYPES),
                "currency_code": "EUR",
                "account_status": "active",
                "available_balance": round(500 + (seed % 19500) + ((seed % 100) / 100), 2),
                "opened_at": opened_at,
                "created_at": opened_at,
                "updated_at": opened_at,
            }
        )

    return pd.DataFrame(rows, columns=ACCOUNT_COLUMNS)


def build_cards(customers: pd.DataFrame, accounts: pd.DataFrame) -> pd.DataFrame:
    account_lookup = accounts.set_index("customer_id")["account_id"].to_dict()
    rows = []

    for customer in customers.itertuples(index=False):
        issued_at = pd.Timestamp(customer.created_at) + pd.Timedelta(days=1)
        card_id = f"CARD-{int(customer.customer_id):06d}" if customer.customer_id.isdigit() else f"CARD-{customer.customer_id}"
        rows.append(
            {
                "card_id": card_id,
                "account_id": account_lookup[customer.customer_id],
                "customer_id": customer.customer_id,
                "card_type": pick(f"card-type-{customer.customer_id}", CARD_TYPES),
                "card_network": pick(f"card-network-{customer.customer_id}", CARD_NETWORKS),
                "card_status": "active",
                "issued_at": issued_at,
                "expiry_date": (issued_at + pd.DateOffset(years=4)).date(),
                "created_at": issued_at,
                "updated_at": issued_at,
            }
        )

    return pd.DataFrame(rows, columns=CARD_COLUMNS)


def build_terminals(terminal_ids: list[str]) -> pd.DataFrame:
    rows = []
    base_date = pd.Timestamp("2017-01-01")

    for terminal_id in terminal_ids:
        seed = stable_int(terminal_id)
        installed_at = base_date + pd.Timedelta(days=seed % 365)
        rows.append(
            {
                "terminal_id": terminal_id,
                "merchant_id": f"MRC-{seed % 2500:04d}",
                "merchant_category": pick(f"merchant-category-{terminal_id}", MERCHANT_CATEGORIES),
                "channel_type": pick(f"channel-{terminal_id}", CHANNEL_TYPES),
                "city": pick(f"city-{terminal_id}", CITIES),
                "terminal_region": pick(f"region-{terminal_id}", CUSTOMER_REGIONS),
                "country_code": COUNTRY_CODE,
                "risk_band": pick(f"risk-{terminal_id}", RISK_TIERS),
                "terminal_status": "active",
                "installed_at": installed_at,
                "created_at": installed_at,
                "updated_at": installed_at,
            }
        )

    return pd.DataFrame(rows, columns=TERMINAL_COLUMNS)


def build_transactions(frame: pd.DataFrame, account_lookup: dict[str, str], card_lookup: dict[str, str]) -> pd.DataFrame:
    transactions = pd.DataFrame(
        {
            "transaction_id": frame["TRANSACTION_ID"].astype("int64"),
            "event_timestamp": frame["TX_DATETIME"],
            "customer_id": frame["CUSTOMER_ID"],
            "account_id": frame["CUSTOMER_ID"].map(account_lookup),
            "card_id": frame["CUSTOMER_ID"].map(card_lookup),
            "terminal_id": frame["TERMINAL_ID"],
            "amount": frame["TX_AMOUNT"].astype(float).round(2),
            "currency_code": "EUR",
            "transaction_type": frame["TRANSACTION_ID"].astype(str).map(lambda value: pick(f"tx-type-{value}", TRANSACTION_TYPES)),
            "channel_type": frame["TERMINAL_ID"].map(lambda value: pick(f"channel-{value}", CHANNEL_TYPES)),
            "auth_status": "approved",
            "tx_time_seconds": frame["TX_TIME_SECONDS"].astype(int),
            "tx_time_days": frame["TX_TIME_DAYS"].astype(int),
            "is_weekend": frame["TX_DATETIME"].dt.dayofweek >= 5,
            "is_night": (frame["TX_DATETIME"].dt.hour < 6) | (frame["TX_DATETIME"].dt.hour >= 22),
            "created_at": frame["TX_DATETIME"],
        }
    )

    return transactions.loc[:, TRANSACTION_COLUMNS]


def build_fraud_cases(frame: pd.DataFrame, transactions: pd.DataFrame) -> pd.DataFrame:
    fraud_frame = frame.loc[frame["TX_FRAUD"] == 1].copy()
    if fraud_frame.empty:
        return pd.DataFrame(columns=FRAUD_CASE_COLUMNS)

    tx_lookup = transactions.set_index("transaction_id")
    rows = []

    for fraud_row in fraud_frame.itertuples(index=False):
        transaction = tx_lookup.loc[int(fraud_row.TRANSACTION_ID)]
        scenario = int(fraud_row.TX_FRAUD_SCENARIO)
        delay_days = SCENARIO_DELAY_DAYS.get(scenario, 14)
        reported_at = pd.Timestamp(transaction["event_timestamp"]) + pd.Timedelta(days=delay_days)
        resolved_at = reported_at + pd.Timedelta(days=max(1, scenario))
        rows.append(
            {
                "case_id": f"CASE-{int(fraud_row.TRANSACTION_ID)}",
                "transaction_id": int(fraud_row.TRANSACTION_ID),
                "customer_id": str(fraud_row.CUSTOMER_ID),
                "card_id": transaction["card_id"],
                "fraud_scenario": scenario,
                "case_status": "confirmed_fraud",
                "resolution_source": SCENARIO_RESOLUTION_SOURCE.get(scenario, "manual_review"),
                "reported_at": reported_at,
                "resolved_at": resolved_at,
                "loss_amount": round(float(transaction["amount"]), 2),
                "created_at": reported_at,
            }
        )

    return pd.DataFrame(rows, columns=FRAUD_CASE_COLUMNS)


def dataframe_to_buffer(dataframe: pd.DataFrame) -> io.StringIO:
    buffer = io.StringIO()
    dataframe.to_csv(buffer, index=False, header=False, na_rep="", date_format="%Y-%m-%d %H:%M:%S")
    buffer.seek(0)
    return buffer


def copy_dataframe(connection: psycopg.Connection, table_name: str, columns: list[str], dataframe: pd.DataFrame) -> None:
    if dataframe.empty:
        return

    buffer = dataframe_to_buffer(dataframe)
    column_list = ", ".join(columns)
    with connection.cursor() as cursor:
        with cursor.copy(
            f"COPY banking.{table_name} ({column_list}) FROM STDIN WITH (FORMAT CSV)"
        ) as copy:
            copy.write(buffer.getvalue())


def truncate_tables(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            TRUNCATE TABLE
                banking.fraud_cases,
                banking.transactions,
                banking.cards,
                banking.accounts,
                banking.terminals,
                banking.customers
            RESTART IDENTITY CASCADE
            """
        )


def validate_loaded_counts(connection: psycopg.Connection) -> dict[str, int]:
    results: dict[str, int] = {}
    with connection.cursor() as cursor:
        for table_name in ["customers", "accounts", "cards", "terminals", "transactions", "fraud_cases"]:
            cursor.execute(f"SELECT COUNT(*) FROM banking.{table_name}")
            results[table_name] = cursor.fetchone()[0]
    return results


def acquire_load_lock(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_try_advisory_lock(%s)", (LOAD_LOCK_ID,))
        lock_acquired = cursor.fetchone()[0]

    if not lock_acquired:
        raise RuntimeError(
            "Another OLTP seed load session is already running. Wait for it to finish before rerunning this script."
        )


def release_load_lock(connection: psycopg.Connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_unlock(%s)", (LOAD_LOCK_ID,))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load normalized banking-style OLTP data into Postgres.")
    parser.add_argument("--data-dir", default="data", help="Directory containing .pkl seed files")
    parser.add_argument("--pattern", default="*.pkl", help="Seed file glob pattern")
    parser.add_argument("--db-host", default="localhost", help="Postgres host")
    parser.add_argument("--db-port", type=int, default=5433, help="Postgres port")
    parser.add_argument("--db-name", default="fraud_bank", help="Postgres database name")
    parser.add_argument("--db-user", default="postgres", help="Postgres user")
    parser.add_argument("--db-password", default="postgres", help="Postgres password")
    parser.add_argument("--limit-files", type=int, default=None, help="Optional number of daily seed files to load")
    parser.add_argument("--skip-reset", action="store_true", help="Append instead of truncating target tables first")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    file_paths = list_seed_files(args.data_dir, args.pattern, args.limit_files)
    print(f"Found {len(file_paths)} seed files")

    customer_ids, terminal_ids = collect_unique_ids(file_paths)
    customers = build_customers(customer_ids)
    accounts = build_accounts(customers)
    cards = build_cards(customers, accounts)
    terminals = build_terminals(terminal_ids)

    account_lookup = accounts.set_index("customer_id")["account_id"].to_dict()
    card_lookup = cards.set_index("customer_id")["card_id"].to_dict()

    connection = psycopg.connect(
        host=args.db_host,
        port=args.db_port,
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
    )

    try:
        acquire_load_lock(connection)

        if not args.skip_reset:
            print("Truncating existing OLTP tables...")
            truncate_tables(connection)
            connection.commit()

        print("Loading dimension tables...")
        copy_dataframe(connection, "customers", CUSTOMER_COLUMNS, customers)
        copy_dataframe(connection, "accounts", ACCOUNT_COLUMNS, accounts)
        copy_dataframe(connection, "cards", CARD_COLUMNS, cards)
        copy_dataframe(connection, "terminals", TERMINAL_COLUMNS, terminals)
        connection.commit()

        total_transactions = 0
        total_fraud_cases = 0

        for index, file_path in enumerate(file_paths, start=1):
            seed_frame = read_seed_frame(file_path)
            transactions = build_transactions(seed_frame, account_lookup, card_lookup)
            fraud_cases = build_fraud_cases(seed_frame, transactions)
            copy_dataframe(connection, "transactions", TRANSACTION_COLUMNS, transactions)
            copy_dataframe(connection, "fraud_cases", FRAUD_CASE_COLUMNS, fraud_cases)
            connection.commit()

            total_transactions += len(transactions)
            total_fraud_cases += len(fraud_cases)
            print(
                f"[{index}/{len(file_paths)}] Loaded {file_path.name}: "
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
    finally:
        try:
            release_load_lock(connection)
            connection.commit()
        except Exception:
            connection.rollback()
        connection.close()


if __name__ == "__main__":
    main()