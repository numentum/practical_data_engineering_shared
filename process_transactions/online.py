import json
import pandas as pd
from datetime import datetime

from practical_data_engineering_shared.io.extract import extract_table
from practical_data_engineering_shared.io.load import load_dataframe
from practical_data_engineering_shared.utils import get_lookup_fn, hash_id
from practical_data_engineering_shared.constants import TAX_RATE


def break_down_total(row, name_to_price):
    """
    Calculates net amount and quantity from the total price given tax rate and unit price.

    Params
    ------
    row : pd.Series
        One row of a pd.DataFrame
    name_to_price : Callable
        Function returned by `get_lookup_fn` that takes a product name and returns its unit_price

    Returns
    -------
    tuple (
        unit_price : float
        quantity: int
        tax : float
    )
    """

    net = row["amount"] / (1 + TAX_RATE)
    unit_price = name_to_price(row["description"])
    quantity = int(net / unit_price)
    tax = round(row["amount"] * TAX_RATE, 2)

    return unit_price, quantity, tax


def transform_online_transactions(df, products_df=None):
    # Gather prerequisites of the operations
    if products_df is None:
        products_df = extract_table("products")

    name_to_sku = get_lookup_fn(products_df, from_col="name", to_col="sku")
    name_to_price = get_lookup_fn(products_df, from_col="name", to_col="unit_price")

    transactions = []
    for i, row in df.iterrows():
        data = json.loads(row["stripe_data"])

        if pd.isna(data):
            continue

        # 8. Break down total to unit_price, quantity and tax given tax rate and product
        unit_price, quantity, tax = break_down_total(data, name_to_price)
        transactions.append(
            {
                # 1. Create unique ID by hashing Stripe transaction ID
                "transaction_id": hash_id(data["id"]),
                # 2. Parse Integer timestamp to Datetime object
                "created_at": datetime.utcfromtimestamp(data["created"]),
                # 3. Add location with constant value 'online'
                "location": "online",
                # 4. Extract product_name from Stripe JSON (named description)
                "product_name": data["description"],
                # 5. Enrich with sku from product_name
                "sku": name_to_sku(data["description"]),
                # 6. Add source with constant value 'online'
                "source": "online",
                # 7. Extract pamynet_method from Stripe JSON (named object)
                "payment_method": data["object"].replace("treasury.received_", ""),
                "unit_price": unit_price,
                "quantity": quantity,
                "tax": tax,
                # 9. Extract total from Stripe JSON (named amount)
                "total": data["amount"],
            }
        )

    return pd.DataFrame(transactions)


def process_online_transactions():
    df = extract_table("online_transactions")
    df = transform_online_transactions(df)
    load_dataframe(df)
