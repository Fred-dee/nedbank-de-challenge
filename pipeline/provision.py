"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  → dim_accounts.account_sk
      fact_transactions.customer_sk → dim_customers.customer_sk
      dim_accounts.customer_id      → dim_customers.customer_id
  - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.
  - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.

See output_schema_spec.md for the complete field-by-field specification.
"""

import logging

from pipeline.extended_config import ExtendedConfig
from pipeline.gold.provision_accounts import build_dim_accounts
from pipeline.gold.provision_customers import build_dim_customers
from pipeline.gold.provision_transactions import build_fact_transactions
from pipeline.pipeline_config import PipelineConfig


def run_provisioning(pipeline_config: PipelineConfig, extended_config: ExtendedConfig):
    #
    # Suggested steps:
    #   1. Load pipeline_config.yaml to get input/output paths.
    #   2. Initialise (or reuse) SparkSession.
    #   3. Read Silver tables.
    #   4. Build dim_customers with surrogate keys and derived age_band.
    #   5. Build dim_accounts with surrogate keys; rename customer_ref → customer_id.
    #   6. Build fact_transactions, resolving account_sk and customer_sk via joins.
    #   7. Write all three Gold tables as Delta Parquet.
    #   8. (Stage 2+) Write dq_report.json to /data/output/.
    silver_root = pipeline_config.get("output.silver_path")
    gold_root = pipeline_config.get("output.gold_path")

    logging.info("Starting Gold layer provisioning")
    logging.info("Silver root: %s", silver_root)
    logging.info("Gold root: %s", gold_root)

    build_dim_customers(silver_root, gold_root, extended_config.get("batch_size.gold.customers"))
    build_dim_accounts(silver_root, gold_root, extended_config.get("batch_size.gold.accounts"))
    build_fact_transactions(silver_root, gold_root, extended_config.get("batch_size.gold.transactions"))

    logging.info("Gold layer provisioning complete")
