def transform_customers(df):
    """Specific rules for customers."""
    if "province" in df.columns:
        df["province"] = df["province"].str.upper().str.strip()
    return df
