import logging

import pyarrow as pa
from deltalake import write_deltalake, DeltaTable

from pipeline.extended_config import ExtendedConfig
from pipeline.pipeline_config import PipelineConfig
from pipeline.silver.transform_accounts import transform_accounts
from pipeline.silver.transform_customers import transform_customers
from pipeline.silver.transform_transactions import transform_transactions

logger = logging.getLogger(__name__)


def _write_to_silver(df, path):
    """Ensures consistent Silver formatting and strict typing."""
    table = pa.Table.from_pandas(df, preserve_index=False)

    if "dq_flag" in table.column_names:
        idx = table.schema.get_field_index("dq_flag")
        table = table.cast(table.schema.set(idx, pa.field("dq_flag", pa.string())))

    write_deltalake(
        path,
        table,
        mode="overwrite",
        configuration={"delta.minReaderVersion": "1", "delta.minWriterVersion": "2"}
    )
    logger.info(f"Wrote Silver table: {path}")


def _process_entity(name, key, config):
    """Refined deduplication engine focusing on natural key stability."""
    bronze_path = f"{config.get('output.bronze_path')}/{name}"
    silver_path = f"{config.get('output.silver_path')}/{name}"

    # 1. Load from Bronze
    df = DeltaTable(bronze_path).to_pyarrow_table().to_pandas()
    initial_count = len(df)

    # 2. Natural Key Hardening
    # Ensure the key column exists and clean it for a robust match
    if key in df.columns:
        # Convert to string and strip whitespace to prevent "hidden" duplicates
        df[key] = df[key].astype(str).str.strip()

        # 3. Deduplication: Stable Sort & Drop
        # We sort by ingestion_timestamp (Descending).
        # If timestamps are identical, the original order is preserved.
        df = df.sort_values(by=["ingestion_timestamp"], ascending=False, kind="stable")

        # Keep the 'first' (which is the most recent due to the sort)
        df = df.drop_duplicates(subset=[key], keep="first")

        deduped_count = len(df)
        dropped = initial_count - deduped_count
        logger.info(f"[{name.upper()}] Natural Key: {key} | Initial: {initial_count} | Dropped: {dropped}")
    else:
        logger.warning(f"[{name.upper()}] Key '{key}' not found in columns! Skipping dedupe.")

    # 4. DISPATCHER: Apply entity-specific logic
    if name == "accounts":
        df = transform_accounts(df)
    elif name == "customers":
        df = transform_customers(df)
    elif name == "transactions":
        df = transform_transactions(df)

    # 5. Write to Silver
    _write_to_silver(df, silver_path)


def run_transformation(pipeline_config: PipelineConfig, extended_config: ExtendedConfig):
    """Entry point: Dispatches entities to their specific logic handlers."""

    entities = {
        "accounts": "account_id",
        "customers": "customer_id",
        "transactions": "transaction_id"
    }

    for entity_name, key_col in entities.items():
        try:
            logger.info(f"--- Starting Silver Transformation: {entity_name} ---")
            _process_entity(entity_name, key_col, pipeline_config)
        except Exception as e:
            logger.error(f"Critical failure transforming {entity_name}: {e}")
            raise
