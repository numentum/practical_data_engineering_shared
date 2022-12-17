import os
import pandas as pd


def extract_table(table_name):
    assert os.getenv("PG_CONN") is not None, "PG_CONN environment variable is not set."
    pg_conn = os.getenv("PG_CONN")

    query = f"SELECT * FROM {table_name}"

    df = pd.read_sql(
        sql=query,
        con=pg_conn,
    )
    
    return df