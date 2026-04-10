import pandas as pd


def transform_transactions(df):
    """Specific rules for transactions (Stage 1 requirements)."""
    # Initialize DQ flag as String type to avoid NullType errors
    df["dq_flag"] = pd.Series([None] * len(df), dtype="object")

    # Castings
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce").dt.date

    # DQ Flagging
    df.loc[~df["currency"].isin(["ZAR", "R"]), "dq_flag"] = "CURRENCY_VARIANT"
    df.loc[df["amount"].isna(), "dq_flag"] = "NULL_REQUIRED"

    # Normalization
    df["currency"] = "ZAR"

    return df
