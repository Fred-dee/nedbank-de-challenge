from pipeline.engine.deltalake_writer import ingest_csv_file_to_delta


def ingest_accounts(input_file_path: str, output_file_path: str, batch_size=10000):
    query = f"SELECT * from read_csv_auto('{input_file_path}')"
    ingest_csv_file_to_delta(query, output_file_path, batch_size)
    pass
