import logging
from pathlib import Path

import pandas as pd

from pipeline.engine.duckdb_session_manager import DuckDBSessionManager
from pipeline.gold.provision_helper import (
    ensure_string_columns,
    generate_sk,
    write_batch,
)
from pipeline.schemas.fact_transactions_schema import fact_transactions_schema
from pipeline.silver.transform_transactions import transform_transactions

logger = logging.getLogger(__name__)



def _read_silver_transactions_in_batches(silver_root: str, batch_size: int):
    """
    Yield Silver transactions as pandas DataFrames in deterministic batches.

    Uses a stable key cursor rather than OFFSET pagination.
    """
    silver_path = Path(silver_root) / "transactions" / "*.parquet"

    ducks = DuckDBSessionManager()

    last_transaction_id = ""

    while True:
        query_string = f"""
            SELECT *
            FROM read_parquet('{silver_path.as_posix()}')
            WHERE transaction_id > '{last_transaction_id}'
            ORDER BY transaction_id
            LIMIT {batch_size}
        """
        batch_df = ducks.query(query_string).fetchdf()

        if batch_df.empty:
            break

        yield batch_df
        last_transaction_id = batch_df["transaction_id"].iloc[-1]


def _load_dim_accounts(gold_root: str) -> pd.DataFrame:
    """
    Load the already-built dim_accounts table for foreign-key resolution.
    """
    gold_path = Path(gold_root) / "dim_accounts" / "*.parquet"
    query_string = f"""
        SELECT account_id, account_sk, customer_id
        FROM read_parquet('{gold_path.as_posix()}')
    """
    ducks = DuckDBSessionManager()
    dim_accounts_df = ducks.query(query_string).fetchdf()
    return dim_accounts_df


def build_fact_transactions(silver_root, gold_root, batch_size: int = 10000):
    logger.info("Building fact_transactions batches from Silver root: %s", silver_root)

    gold_path = Path(gold_root) / "fact_transactions"
    gold_path.parent.mkdir(parents=True, exist_ok=True)

    dim_accounts_df = _load_dim_accounts(gold_root)

    first_batch = True

    for batch_num, batch_df in enumerate(
        _read_silver_transactions_in_batches(silver_root, batch_size),
        start=1,
    ):
        logger.info("Read fact_transactions batch %s with %s rows", batch_num, len(batch_df))

        required_cols = ["transaction_id", "account_id"]
        missing = [col for col in required_cols if col not in batch_df.columns]
        if missing:
            raise ValueError(f"Silver transactions table missing required columns: {missing}")

        batch_df = ensure_string_columns(batch_df, ["transaction_id", "account_id"])

        batch_df["transaction_sk"] = generate_sk(batch_df["transaction_id"])

        batch_df = batch_df.merge(
            dim_accounts_df,
            on="account_id",
            how="left",
            suffixes=("", "_dim"),
        )

        if "customer_sk" not in batch_df.columns:
            batch_df["customer_sk"] = pd.NA

        orphan_mask = batch_df["account_sk"].isna()
        batch_df.loc[orphan_mask, "dq_flag"] = "ORPHANED_ACCOUNT"

        batch_df["account_sk"] = batch_df["account_sk"].fillna(-1).astype("int64")
        batch_df["customer_sk"] = batch_df["customer_sk"].fillna(-1).astype("int64")

        if "transaction_timestamp" not in batch_df.columns or batch_df["transaction_timestamp"].isna().any():
            batch_df["transaction_timestamp"] = pd.to_datetime(batch_df["transaction_date"])

        write_batch(batch_df, str(gold_path), first_batch, fact_transactions_schema)
        first_batch = False

        logger.info("Wrote fact_transactions batch %s (%s rows)", batch_num, len(batch_df))

    if first_batch:
        empty_df = pd.DataFrame(columns=[field.name for field in fact_transactions_schema])
        write_batch(empty_df, str(gold_path), True, fact_transactions_schema)
        logger.info("Silver transactions was empty; wrote empty fact_transactions table")

    logger.info("fact_transactions build complete: %s", gold_path)
