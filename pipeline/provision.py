# """
# Gold layer: Join and aggregate Silver tables into the scored output schema.
#
# Input paths (Silver layer output — read these, do not modify):
#   /data/output/silver/accounts/
#   /data/output/silver/transactions/
#   /data/output/silver/customers/
#
# Output paths (your pipeline must create these directories):
#   /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
#   /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
#   /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)
#
# Requirements:
#   - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
#     across pipeline re-runs on the same input data. Use row_number() with a
#     stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
#   - Resolve all foreign key relationships:
#       fact_transactions.account_sk  → dim_accounts.account_sk
#       fact_transactions.customer_sk → dim_customers.customer_sk
#       dim_accounts.customer_id      → dim_customers.customer_id
#   - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
#   - Derive dim_customers.age_band from dob (do not copy dob directly).
#   - Write each table as a Delta Parquet table.
#   - Do not hardcode file paths — read from config/pipeline_config.yaml.
#   - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.
#
# See output_schema_spec.md for the complete field-by-field specification.
# """
#
# import pandas as pd
# import pyarrow as pa
# import logging
# import hashlib
# from deltalake import write_deltalake, DeltaTable
# from pipeline.config_helper import PipelineConfig
# from pipeline.gold.provision_accounts import build_dim_accounts
# from pipeline.gold.provision_customers import build_dim_customers
# from pipeline.gold.provision_transactions import build_fact_transactions
#
#
# def _write_gold(df, path):
#     table = pa.Table.from_pandas(df, preserve_index=False)
#     # Final check for NullType on dq_flag
#     if "dq_flag" in table.column_names:
#         idx = table.schema.get_field_index("dq_flag")
#         table = table.cast(table.schema.set(idx, pa.field("dq_flag", pa.string())))
#
#     write_deltalake(
#         path,
#         table,
#         mode="overwrite",
#         # Force Protocol Version 1 (Reader) and 2 (Writer)
#         # This is the 'Golden Ratio' for DuckDB 0.10.0 compatibility
#         configuration={
#             "delta.minReaderVersion": "1",
#             "delta.minWriterVersion": "2"
#         }
#     )
#     logging.info(f"Gold table written: {path}")
#
# def run_provisioning():
#     # TODO: Implement Gold layer provisioning.
#     #
#     # Suggested steps:
#     #   1. Load pipeline_config.yaml to get input/output paths.
#     #   2. Initialise (or reuse) SparkSession.
#     #   3. Read Silver tables.
#     #   4. Build dim_customers with surrogate keys and derived age_band.
#     #   5. Build dim_accounts with surrogate keys; rename customer_ref → customer_id.
#     #   6. Build fact_transactions, resolving account_sk and customer_sk via joins.
#     #   7. Write all three Gold tables as Delta Parquet.
#     #   8. (Stage 2+) Write dq_report.json to /data/output/.
#     config = PipelineConfig()
#     silver_root = config.get("output.silver_path")
#     gold_root = config.get("output.gold_path")
#
#     # 1. Load Silver Tables
#     logging.info("Loading Silver tables for Gold provisioning...")
#     df_tx = DeltaTable(f"{silver_root}/transactions").to_pyarrow_table().to_pandas()
#     df_acc = DeltaTable(f"{silver_root}/accounts").to_pyarrow_table().to_pandas()
#     df_cust = DeltaTable(f"{silver_root}/customers").to_pyarrow_table().to_pandas()
#
#     # 2. Build Dimensions (Sequential to save memory)
#     dim_cust = build_dim_customers(df_cust)
#     dim_acc = build_dim_accounts(df_acc, dim_cust)
#
#     # 3. Build Fact
#     fact_tx = build_fact_transactions(df_tx, dim_acc, dim_cust)
#
#     # 4. Final Write
#     for name, df in [("dim_customers", dim_cust),
#                      ("dim_accounts", dim_acc),
#                      ("fact_transactions", fact_tx)]:
#         _write_gold(df, f"{gold_root}/{name}")
import logging
from datetime import datetime, timezone

import pyarrow as pa
from deltalake import write_deltalake

from pipeline.config_helper import PipelineConfig
from pipeline.engine import duckdb_session_manager

logger = logging.getLogger(__name__)


def run_provisioning():
    config = PipelineConfig()
    ducks = duckdb_session_manager.DuckDBSessionManager().get_connection()

    # Define paths
    silver_root = config.get("output.silver_path")
    gold_root = config.get("output.gold_path")

    # 1. Register Silver data as Parquet views (Bypasses delta_scan)
    # DuckDB can read the underlying parquet files directly from the silver folders
    ducks.execute(
        f"CREATE OR REPLACE VIEW silver_tx AS SELECT * FROM read_parquet('{silver_root}/transactions/*.parquet')")
    ducks.execute(
        f"CREATE OR REPLACE VIEW silver_acc AS SELECT * FROM read_parquet('{silver_root}/accounts/*.parquet')")
    ducks.execute(
        f"CREATE OR REPLACE VIEW silver_cust AS SELECT * FROM read_parquet('{silver_root}/customers/*.parquet')")

    tables_to_provision = {
        "dim_customers": """
                         SELECT (hash(customer_id) & 9223372036854775807)::BIGINT as customer_sk, customer_id,
                                gender,
                                province,
                                income_band,
                                segment,
                                CAST(risk_score AS INT) as risk_score,
                                kyc_status,
                                'Unknown'               as age_band
                         FROM silver_cust""",

        "dim_accounts": """
                        SELECT (hash(account_id) & 9223372036854775807)::BIGINT as account_sk, account_id,
                               customer_ref                            as customer_id,
                               account_type,
                               account_status,
                               CAST(open_date AS DATE)                 as open_date,
                               product_tier,
                               digital_channel,
                               CAST(credit_limit AS DECIMAL(18, 2))    as credit_limit,
                               CAST(current_balance AS DECIMAL(18, 2)) as current_balance,
                               CAST(last_activity_date AS DATE)        as last_activity_date
                        FROM silver_acc""",

        "fact_transactions": """
                             SELECT (hash(t.transaction_id) & 9223372036854775807)::BIGINT as transaction_sk, t.transaction_id,
                                    (hash(t.account_id) & 9223372036854775807)::BIGINT as account_sk, COALESCE((hash(a.customer_ref) & 9223372036854775807)::BIGINT, -1) as customer_sk,
                                    CAST(t.transaction_date AS DATE)                                   as transaction_date,
                                    CAST(t.transaction_date || ' ' || t.transaction_time AS TIMESTAMP) as transaction_timestamp,
                                    t.transaction_type,
                                    t.merchant_category,
                                    CAST(t.amount AS DECIMAL(18, 2))                                   as amount,
                                    t.currency,
                                    t.channel,
                                    c.province,
                                    t.dq_flag,
                                    t.ingestion_timestamp
                             FROM silver_tx t
                                      LEFT JOIN silver_acc a ON t.account_id = a.account_id
                                      LEFT JOIN silver_cust c ON a.customer_ref = c.customer_id
                             """
    }

    for table_name, query in tables_to_provision.items():
        output_path = f"{gold_root}/{table_name}"
        logger.info(f"Provisioning Gold Table: {table_name}")
        _stream_query_to_delta(ducks, query, output_path)


def _stream_query_to_delta(ducks, query, output_path, batch_size=50000):
    query_ref = ducks.execute(query)
    reader = query_ref.fetch_record_batch(batch_size)

    first_batch = True
    for batch in reader:
        table = pa.Table.from_batches([batch])
        # This call creates the correct _delta_log folder
        write_deltalake(
            output_path,
            table,
            mode="overwrite" if first_batch else "append",
            configuration={
                "delta.minReaderVersion": "1",
                "delta.minWriterVersion": "2"
            }
        )
        first_batch = False
