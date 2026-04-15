import logging

import pyarrow as pa
from deltalake import write_deltalake

from pipeline.engine.duckdb_session_manager import DuckDBSessionManager
from pipeline.extended_config import ExtendedConfig
from pipeline.pipeline_config import PipelineConfig
from pipeline.silver.transform_accounts import transform_accounts
from pipeline.silver.transform_customers import transform_customers
from pipeline.silver.transform_transactions import transform_transactions
from pipeline.timing_helper import Timer

logger = logging.getLogger(__name__)


def _write_to_silver(df, path, first_batch: bool):
    """Ensures consistent Silver formatting and strict typing."""
    table = pa.Table.from_pandas(df, preserve_index=False)

    if "dq_flag" in table.column_names:
        idx = table.schema.get_field_index("dq_flag")
        table = table.cast(table.schema.set(idx, pa.field("dq_flag", pa.string())))

    write_deltalake(
        path,
        table,
        mode="overwrite" if first_batch else "append",
        configuration={"delta.minReaderVersion": "1", "delta.minWriterVersion": "2"},
    )
    logger.info("Wrote Silver table: %s", path)


def _read_bronze_entity_in_batches(bronze_path: str, key_col: str, batch_size: int):
    """
    Yield deduplicated Bronze batches ordered by the natural key.

    DuckDB performs:
      - parquet scan
      - key normalization
      - deduplication via row_number()
      - cursor-based pagination
    """
    ducks = DuckDBSessionManager()
    last_key = ""

    while True:
        query = f"""
            WITH deduped AS (
                SELECT *
                FROM (
                    SELECT
                        *,
                        row_number() OVER (
                            PARTITION BY {key_col}
                            ORDER BY ingestion_timestamp DESC
                        ) AS rn
                    FROM read_parquet('{bronze_path}/*.parquet')
                    WHERE {key_col} > '{last_key}'
                )
                WHERE rn = 1
            )
            SELECT *
            FROM deduped
            ORDER BY {key_col}
            LIMIT {batch_size}
        """
        batch_df = ducks.query(query).fetchdf()

        if batch_df.empty:
            break

        yield batch_df
        last_key = str(batch_df[key_col].iloc[-1])

    ducks.close()


def _process_entity(name, key_col, config: PipelineConfig, batch_size: int):
    bronze_path = f"{config.get('output.bronze_path')}/{name}"
    silver_path = f"{config.get('output.silver_path')}/{name}"

    logger.info("Starting Silver Transformation: %s", name)

    first_batch = True
    batch_count = 0

    logger.info("Processing batches for %s with batch size %s", name, batch_size)
    for batch_df in _read_bronze_entity_in_batches(bronze_path, key_col, batch_size):
        batch_count += 1
        initial_count = len(batch_df)

        if key_col not in batch_df.columns:
            raise ValueError(f"[{name.upper()}] Key '{key_col}' not found in batch columns")

        batch_df[key_col] = batch_df[key_col].astype(str).str.strip()

        if name == "accounts":
            batch_df = transform_accounts(batch_df)
        elif name == "customers":
            batch_df = transform_customers(batch_df)
        elif name == "transactions":
            batch_df = transform_transactions(batch_df)

        _write_to_silver(batch_df, silver_path, first_batch)
        first_batch = False

        logger.info(
            "[%s] Batch %s processed | Rows: %s",
            name.upper(),
            batch_count,
            initial_count,
        )

    logger.info("[%s] Transformation complete", name.upper())


def run_transformation(pipeline_config: PipelineConfig, extended_config: ExtendedConfig):
    """Entry point: Dispatches entities to their specific logic handlers."""

    entities = {
        "accounts": "account_id",
        "customers": "customer_id",
        "transactions": "transaction_id",
    }

    batch_sizes = {
        "accounts": extended_config.get("batch_size.silver.accounts"),
        "customers": extended_config.get("batch_size.silver.customers"),
        "transactions": extended_config.get("batch_size.silver.transactions"),
    }

    for entity_name, key_col in entities.items():
        try:
            logger.info("--- Starting Silver Transformation: %s ---", entity_name)
            with Timer(f"Silver Transformation: {entity_name}"):
                _process_entity(entity_name, key_col, pipeline_config, batch_sizes[entity_name])
        except Exception as e:
            logger.error("Critical failure transforming %s: %s", entity_name, e)
            raise
