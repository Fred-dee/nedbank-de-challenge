import logging

from pipeline.gold.provision_helper import generate_sk


def build_dim_customers(df):
    logging.info("Building dim_customers...")
    # Derive Age Band (Placeholder logic for Stage 1)
    # You would typically use (today - dob)
    df["age_band"] = "Unknown"
    df["customer_sk"] = generate_sk(df["customer_id"])
    return df
