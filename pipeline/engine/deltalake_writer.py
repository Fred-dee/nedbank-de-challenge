from datetime import datetime, timezone

import pyarrow as pa
from deltalake import write_deltalake

from pipeline.engine import duckdb_session_manager


def ingest_csv_file_to_delta(query: str, output_path: str, batch_size=10000):
    ducks = duckdb_session_manager.DuckDBSessionManager()
    ingestion_timestamp = datetime.now(timezone.utc)
    query_ref = ducks.query(query)

    reader = query_ref.fetch_record_batch(batch_size)
    first_batch = True

    for batch in reader:
        table = pa.Table.from_batches([batch])
        table = table.append_column("ingestion_timestamp", pa.array([ingestion_timestamp] * table.num_rows))

        write_deltalake(output_path, table, mode="overwrite" if first_batch else "append")
        first_batch = False


def ingest_json_file_to_delta(query: str, output_path: str, batch_size=10000):
    ducks = duckdb_session_manager.DuckDBSessionManager()
    ingestion_timestamp = datetime.now(timezone.utc)
    query_ref = ducks.query(query)

    reader = query_ref.fetch_record_batch(batch_size)
    first_batch = True

    for batch in reader:
        table = pa.Table.from_batches([batch])
        table = table.append_column("ingestion_timestamp", pa.array([ingestion_timestamp] * table.num_rows))

        write_deltalake(output_path, table, mode="overwrite" if first_batch else "append")
        first_batch = False
