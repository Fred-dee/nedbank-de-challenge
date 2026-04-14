"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Input paths (read-only mounts — do not write here):
  /data/input/accounts.csv
  /data/input/transactions.jsonl
  /data/input/customers.csv

Output paths (your pipeline must create these directories):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Requirements:
  - Preserve source data as-is; do not transform at this layer.
  - Add an `ingestion_timestamp` column (TIMESTAMP) recording when each
    record entered the Bronze layer. Use a consistent timestamp for the
    entire ingestion run (not per-row).
  - Write each table as a Delta Parquet table (not plain Parquet).
  - Read paths from config/pipeline_config.yaml — do not hardcode paths.
  - All paths are absolute inside the container (e.g. /data/input/accounts.csv).

Spark configuration tip:
  Run Spark in local[2] mode to stay within the 2-vCPU resource constraint.
  Configure Delta Lake using the builder pattern shown in the base image docs.
"""

from pipeline.bronze.ingest_accounts import ingest_accounts
from pipeline.bronze.ingest_customers import ingest_customers
from pipeline.bronze.ingest_transactions import ingest_transactions
from pipeline.extended_config import ExtendedConfig
from pipeline.pipeline_config import PipelineConfig
from pipeline.timing_helper import Timer


def run_ingestion(pipeline_config: PipelineConfig, extended_config: ExtendedConfig):
    # TODO: Implement Bronze layer ingestion.
    #
    # Suggested steps:
    #   1. Load pipeline_config.yaml to get input/output paths.
    #   2. Initialise a SparkSession with Delta Lake support (local[2]).
    #   3. Read accounts.csv → append ingestion_timestamp → write to bronze/accounts/.
    #   4. Read transactions.jsonl → append ingestion_timestamp → write to bronze/transactions/.
    #   5. Read customers.csv → append ingestion_timestamp → write to bronze/customers/.

    account_csv_path = pipeline_config.get("input.accounts_path")
    account_output_path = pipeline_config.get("output.bronze_path") + "/accounts/"

    customer_csv_path = pipeline_config.get("input.customers_path")
    customer_output_path = pipeline_config.get("output.bronze_path") + "/customers/"

    transaction_jsonl_path = pipeline_config.get("input.transactions_path")
    transaction_output_path = pipeline_config.get("output.bronze_path") + "/transactions/"

    with Timer("Ingest Accounts Timer"):
        ingest_accounts(account_csv_path, account_output_path, extended_config.get("batch_size.bronze.accounts"))
    with Timer("Ingest Customers Timer"):
        ingest_customers(customer_csv_path, customer_output_path, extended_config.get("batch_size.bronze.customers"))
    with Timer("Ingest Transactions Timer"):
        ingest_transactions(transaction_jsonl_path, transaction_output_path, extended_config.get("batch_size.bronze.transactions"))
