import pyarrow as pa

fact_transactions_schema = pa.schema([
    ("transaction_sk", pa.int64(), False),
    ("transaction_id", pa.string(), False),
    ("account_sk", pa.int64(), False),
    ("customer_sk", pa.int64(), False),
    ("transaction_date", pa.date32(), False),
    ("transaction_timestamp", pa.timestamp("us"), False),
    ("transaction_type", pa.string(), False),
    ("merchant_category", pa.string(), True),
    ("merchant_subcategory", pa.string(), True),
    ("amount", pa.decimal128(18, 2), False),
    ("currency", pa.string(), False),
    ("channel", pa.string(), False),
    ("province", pa.string(), True),
    ("dq_flag", pa.string(), True),
    ("ingestion_timestamp", pa.timestamp("us"), False)
])
