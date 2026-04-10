import pyarrow as pa

dim_accounts_schema = pa.schema([
    ("account_sk", pa.int64(), False),
    ("account_id", pa.string(), False),
    ("customer_id", pa.string(), False),
    ("account_type", pa.string(), False),
    ("account_status", pa.string(), False),
    ("open_date", pa.date32(), False),
    ("product_tier", pa.string(), False),
    ("digital_channel", pa.string(), False),
    ("credit_limit", pa.decimal128(18, 2), True),
    ("current_balance", pa.decimal128(18, 2), False),
    ("last_activity_date", pa.date32(), True),
])
