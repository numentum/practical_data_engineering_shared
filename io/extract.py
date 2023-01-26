import os
import pandas as pd


def extract_table(table_name, pg_conn=None, start_time=None, end_time=None, context=None):
    """
    Params
    ------
    table_name : str
        Name of the PostgreSQL table to extract. The PostgreSQL connection is loaded from the `PG_CONN` environment variable.

    pg_conn : str, optional
        Optional PostgreSQL connection string. If not provided, PG_CONN environment variable is used.

    start_time : str, optional
        Datetime formatted as `%Y-%d-%m`. If provided, only records with a timestamp greater than or equal to this value are returned. Used for partitioning.

    end_time : str, optional
        Datetime formatted as `%Y-%d-%m`. If provided, only records with a timestamp smaller than or equal to this value are returned. Used for partitioning.

    context : dagster.core.execution.context.compute.ComputeExecutionContext, optional
        Dagster context object. Used to log information about the processing.

    Returns
    -------
    df : pd.DataFrame
        Pandas DataFrame containing the whole PostgreSQL table.

    Raises
    ------
    AssertionError
        If the PG_CONN environment variable is not set.
    """
    if pg_conn is None:
        assert os.getenv("PG_CONN") is not None, "PG_CONN environment variable is not set."
        pg_conn = os.getenv("PG_CONN")

    query = f"SELECT * FROM {table_name}"

    if start_time is not None and end_time is not None:
        if table_name == "online_transactions":
            start_timestamp = pd.to_datetime(start_time).timestamp()
            end_timestamp = pd.to_datetime(end_time).timestamp()

            query += f" WHERE stripe_data ->> 'created' >= '{start_timestamp}' AND stripe_data ->> 'created' < '{end_timestamp}'"

        else:
            query += f" WHERE created_at >= '{start_time}' AND created_at < '{end_time}'"

    df = pd.read_sql(
        sql=query,
        con=pg_conn,
    )

    return df
