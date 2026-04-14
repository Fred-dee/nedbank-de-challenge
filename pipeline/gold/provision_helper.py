import hashlib
from typing import Iterable

import pandas as pd
import pyarrow as pa
from deltalake import write_deltalake


def _normalize_key_value(value) -> str:
    """
    Normalize a natural-key value before hashing so SK generation is stable.

    Rules:
      - None / NaN become empty string
      - everything else is stringified and stripped
    """
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def generate_sk(series: pd.Series) -> pd.Series:
    """
    Generate a stable 64-bit integer surrogate key from a single natural-key column.
    """
    normalized = series.map(_normalize_key_value)

    return normalized.apply(
        lambda x: int(hashlib.sha256(x.encode("utf-8")).hexdigest(), 16) % (2 ** 63)
    )


def generate_composite_sk(df: pd.DataFrame, columns: Iterable[str], separator: str = "|") -> pd.Series:
    """
    Generate a stable 64-bit integer surrogate key from multiple natural-key columns.

    Example:
        generate_composite_sk(df, ["account_id", "transaction_date"])
    """
    cols = list(columns)

    if not cols:
        raise ValueError("columns must contain at least one field name")

    normalized_parts = []
    for col in cols:
        if col not in df.columns:
            raise KeyError(f"Missing required key column: {col}")
        normalized_parts.append(df[col].map(_normalize_key_value))

    composite = normalized_parts[0]
    for part in normalized_parts[1:]:
        composite = composite + separator + part

    return composite.apply(
        lambda x: int(hashlib.sha256(x.encode("utf-8")).hexdigest(), 16) % (2 ** 63)
    )


def ensure_string_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """
    Cast selected columns to pandas string/object-safe values for consistent hashing.
    """
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype("string")
    return df


def drop_raw_key_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """
    Drop columns that should not appear in the final Gold output.
    """
    cols = [col for col in columns if col in df.columns]
    if cols:
        df = df.drop(columns=cols)
    return df


def write_batch(df: pd.DataFrame, output_path: str, first_batch: bool, schema) -> None:
    table = _normalize_to_schema(df, schema)

    write_deltalake(
        output_path,
        table,
        mode="overwrite" if first_batch else "append"
    )

def _normalize_to_schema(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """
    Reorder columns to match schema and cast to the expected Arrow types.
    """
    for field in schema:
        if field.name not in df.columns:
            df[field.name] = None

    df = df[[field.name for field in schema]]
    table = pa.Table.from_pandas(df, preserve_index=False)

    if table.schema != schema:
        table = table.cast(schema)

    return table