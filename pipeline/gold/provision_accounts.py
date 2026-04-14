import logging
from pathlib import Path

import pandas as pd

from pipeline.engine.duckdb_session_manager import DuckDBSessionManager
from pipeline.gold.provision_helper import (
    drop_raw_key_columns,
    ensure_string_columns,
    generate_sk,
    write_batch,
)
from pipeline.schemas.dim_accounts_schema import dim_accounts_schema
from pipeline.silver.transform_accounts import transform_accounts

log = logging.getLogger(__name__)


def _read_silver_accounts_in_batches(silver_root: str, batch_size: int):
    """
    Yield Silver accounts as pandas DataFrames in deterministic batches.

    Pagination uses a stable key cursor rather than OFFSET for better behavior
    on larger tables.
    """
    silver_path = Path(silver_root) / "accounts" / "*.parquet"
    ducks = DuckDBSessionManager()

    last_account_id = ""

    while True:
        query_string = f"""
            SELECT *
            FROM read_parquet('{silver_path.as_posix()}')
            WHERE account_id > '{last_account_id}'
            ORDER BY account_id
            LIMIT {batch_size}
        """
        batch_df = ducks.query(query_string).fetchdf()

        if batch_df.empty:
            break

        yield batch_df
        last_account_id = batch_df["account_id"].iloc[-1]


def build_dim_accounts(silver_root, gold_root, batch_size: int = 10000):
    log.info("Building dim_accounts batches from Silver root: %s", silver_root)

    gold_path = Path(gold_root) / "dim_accounts"
    gold_path.parent.mkdir(parents=True, exist_ok=True)

    first_batch = True

    for batch_num, batch_df in enumerate(
        _read_silver_accounts_in_batches(silver_root, batch_size),
        start=1,
    ):
        log.info("Read dim_accounts batch %s with %s rows", batch_num, len(batch_df))

        required_cols = ["account_id", "customer_ref"]
        missing = [col for col in required_cols if col not in batch_df.columns]
        if missing:
            raise ValueError(f"Silver accounts table missing required columns: {missing}")

        batch_df = ensure_string_columns(batch_df, ["account_id", "customer_ref"])
        batch_df = transform_accounts(batch_df)

        batch_df = batch_df.rename(columns={"customer_ref": "customer_id"})
        batch_df["account_sk"] = generate_sk(batch_df["account_id"])

        batch_df = drop_raw_key_columns(batch_df, [])

        write_batch(batch_df, str(gold_path), first_batch, dim_accounts_schema)
        first_batch = False

        log.info("Wrote dim_accounts batch %s (%s rows)", batch_num, len(batch_df))

    if first_batch:
        empty_df = pd.DataFrame(columns=[field.name for field in dim_accounts_schema])
        write_batch(empty_df, str(gold_path), True, dim_accounts_schema)
        log.info("Silver accounts was empty; wrote empty dim_accounts table")

    log.info("dim_accounts build complete: %s", gold_path)
