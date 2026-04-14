import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake

from pipeline.engine.duckdb_session_manager import DuckDBSessionManager
from pipeline.gold.provision_helper import generate_sk, drop_raw_key_columns, write_batch
from pipeline.schemas.dim_customers_schema import dim_customers_schema

logger = logging.getLogger(__name__)


def _derive_age_band(dob_value):
    """
    Derive an age band from DOB.

    Adjust the band labels/ranges here if output_schema_spec.md requires
    different values.
    """
    if dob_value is None or pd.isna(dob_value):
        return None

    if isinstance(dob_value, pd.Timestamp):
        dob = dob_value.date()
    elif isinstance(dob_value, datetime):
        dob = dob_value.date()
    elif isinstance(dob_value, date):
        dob = dob_value
    else:
        dob = pd.to_datetime(dob_value, errors="coerce")
        if pd.isna(dob):
            return None
        dob = dob.date()

    today = date.today()
    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

    if age < 18:
        return "UNDER_18"
    if age < 25:
        return "18_24"
    if age < 35:
        return "25_34"
    if age < 45:
        return "35_44"
    if age < 55:
        return "45_54"
    if age < 65:
        return "55_64"
    return "65_PLUS"



def build_dim_customers(silver_root, gold_root, batch_size: int = 10000):
    """
    Build dim_customers from the Silver customers table in batches.

    This version only handles reading batches from Silver.
    """
    logger.info("Building dim_customers batches from Silver root: %s", silver_root)
    gold_path = Path(gold_root) / "dim_customers"

    first_batch = True

    for batch_num, batch_df in enumerate(_read_silver_customers_in_batches(silver_root, batch_size), start=1):
        logger.info("Read dim_customers batch %s with %s rows", batch_num, len(batch_df))
        if "customer_id" not in batch_df.columns:
            raise ValueError("Silver customers table must contain customer_id")

        batch_df["customer_id"] = batch_df["customer_id"].astype("string")
        batch_df["customer_sk"] = generate_sk(batch_df["customer_id"])
        if "dob" in batch_df.columns:
            batch_df["age_band"] = batch_df["dob"].map(_derive_age_band)
            batch_df = drop_raw_key_columns(batch_df, ["dob"])
        else:
            batch_df["age_band"] = None
        write_batch(batch_df, str(gold_path), first_batch, dim_customers_schema)
        first_batch = False
        logger.info("Wrote dim_customers batch %s (%s rows)", batch_num, len(batch_df))

    if first_batch:
        empty_df = pd.DataFrame(columns=[field.name for field in dim_customers_schema])
        write_batch(empty_df, str(gold_path), True, dim_customers_schema)
        logger.info("Silver customers was empty; wrote empty dim_customers table")
    logger.info("dim_customers build complete: %s", gold_path)

def _read_silver_customers_in_batches(silver_root: str,  batch_size: int):
    """
    Yield Silver customers as pandas DataFrames in deterministic batches.

    Uses DuckDB to read the Delta table through the local session manager.
    """
    silver_path = Path(silver_root) / "customers" / "*.parquet"
    ducks = DuckDBSessionManager()
    last_id = ""

    while True:
        query_string = f"""
            SELECT *
            FROM read_parquet('{silver_path}')
            where customer_id > '{last_id}'
            ORDER BY customer_id
            LIMIT {batch_size}
        """
        batch_df = ducks.query(query_string).fetchdf()

        if batch_df.empty:
            break

        yield batch_df
        last_id = batch_df['customer_id'].max()