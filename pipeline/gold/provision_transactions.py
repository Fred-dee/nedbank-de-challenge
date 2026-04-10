import logging

from pipeline.gold.provision_helper import generate_sk


def build_fact_transactions(df_tx, dim_acc, dim_cust):
    logging.info("Building fact_transactions...")
    df_tx["transaction_sk"] = generate_sk(df_tx["transaction_id"])

    # Resolve account_sk
    df_tx = df_tx.merge(dim_acc[["account_id", "account_sk", "customer_sk"]], on="account_id", how="left")

    # Resolve ORPHANED_ACCOUNT (Stage 1 Requirement)
    df_tx.loc[df_tx["account_sk"].isna(), "dq_flag"] = "ORPHANED_ACCOUNT"

    # Fill missing SKs with -1 (Standard DW practice for orphans)
    df_tx["account_sk"] = df_tx["account_sk"].fillna(-1).astype(int)
    df_tx["customer_sk"] = df_tx["customer_sk"].fillna(-1).astype(int)
    return df_tx
