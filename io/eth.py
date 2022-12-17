import os
import pandas as pd
import pandas_datareader as web

import yfinance as yf
yf.pdr_override()


def get_eth_price():
    """
    Check the range of dates in the `crypto_transactions` table and retrieves ETH prices between the min and max from Yahoo
    using Pandas DataReader.

    Returns
    -------
    eth_price_df : pd.DataFrame
        DataFrame containing ETH prices between the min and max `created_at` values of the crypto transactions.
    """

    date_range = pd.read_sql(
        sql="SELECT min(created_at), max(created_at) FROM crypto_transactions",
        con=os.getenv("PG_CONN"),
    )

    eth_price_df = web.data.get_data_yahoo('ETH-USD', date_range.iloc[0]["min"], date_range.iloc[0]["max"])

    return eth_price_df