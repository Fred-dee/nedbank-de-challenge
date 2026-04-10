import pandas as pd


def transform_accounts(df):
    """Specific rules for accounts."""
    df["open_date"] = pd.to_datetime(df["open_date"], errors="coerce").dt.date
    return df
