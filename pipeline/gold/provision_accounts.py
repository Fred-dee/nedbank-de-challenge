import logging

from pipeline.gold.provision_helper import generate_sk


def build_dim_accounts(df_acc, dim_cust):
    logging.info("Building dim_accounts...")
    # Rename customer_ref -> customer_id
    df_acc = df_acc.rename(columns={"customer_ref": "customer_id"})
    df_acc["account_sk"] = generate_sk(df_acc["account_id"])

    # Join to get customer_sk for the foreign key relationship
    df_acc = df_acc.merge(dim_cust[["customer_id", "customer_sk"]], on="customer_id", how="left")
    return df_acc
