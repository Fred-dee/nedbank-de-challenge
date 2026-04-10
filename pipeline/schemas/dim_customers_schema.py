import pyarrow as pa

dim_customers_schema = pa.schema([
    ("customer_sk", pa.int64(), False),
    ("customer_id", pa.string(), False),
    ("gender", pa.string(), False),
    ("province", pa.string(), False),
    ("income_band", pa.string(), False),
    ("segment", pa.string(), False),
    ("risk_score", pa.int32(), False),
    ("kyc_status", pa.string(), False),
    ("age_band", pa.string(), False),
])
