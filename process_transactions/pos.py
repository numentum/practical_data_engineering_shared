from practical_data_engineering_shared.io.extract import extract_table
from practical_data_engineering_shared.io.load import load_dataframe
from practical_data_engineering_shared.utils import get_lookup_fn, hash_id


def transform_pos_transactions(df, products_df=None):
    # Gather prerequisites of the operations
    if products_df is None:
        products_df = extract_table("products")

    sku_to_name = get_lookup_fn(products_df, from_col="sku", to_col="name")

    # 1. Create a unique ID by hashing
    df["transaction_id"] = df["transaction_id"].apply(hash_id)

    # 2. Enrich data from products table with product name
    df["product_name"] = df["sku"].apply(sku_to_name)

    # 3. Add source column with constant value 'POS'
    df["source"] = "POS"

    return df


def process_pos_transactions():
    df = extract_table("pos_transactions")
    df = transform_pos_transactions(df)
    load_dataframe(df)
