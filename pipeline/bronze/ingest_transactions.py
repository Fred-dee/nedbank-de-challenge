from pipeline.engine.deltalake_writer import ingest_csv_file_to_delta


def ingest_transactions(input_file_path: str, output_file_path: str, batch_size=10000):
    query = f"SELECT * exclude transaction_time, transaction_time::varchar as transaction_time from read_json_auto('{input_file_path}', format='newline_delimited')"
    ingest_csv_file_to_delta(query, output_file_path, batch_size)
    pass
