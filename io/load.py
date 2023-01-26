import os
from sqlalchemy.dialects.postgresql import insert, JSONB


def postgres_upsert(table, conn, keys, data_iter):
    """
    Used as a callable in pd.DataFarme.to_sql's method attribute. The table needs to include a primary key named
    `[TABLE_NAME]_pkey`
    """

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)


def load_dataframe(df, pg_conn=None, table_name="transactions", context=None):
    """
    Inserts a DataFrame to a postgres table in a way that if there is already a record with a matching primary key,
    it will overwrite the record with the new data. This makes it possible to repeatedly call this function to update
    the processed data in case the processing logic changed, without duplicating rows.

    Params
    ------
    df : pd.DataFrame
        Pandas DataFrame to be saved to the `transactions` table.

    pg_conn : str, optional
        Optional PostgreSQL connection string. If not provided, PG_CONN environment variable is used.

    table_name : str, optional
        Name of the PostgreSQL table to load the data to. Defaults to `transactions`.

    context : dagster.core.execution.context.compute.ComputeExecutionContext, optional
        Dagster context object. Used to log information about the processing.

    Raises
    ------
    AssertionError
        If the PG_CONN environment variable is not set.
    """
    if pg_conn is None:
        assert os.getenv("PG_CONN") is not None, "PG_CONN environment variable is not set."
        pg_conn = os.getenv("PG_CONN")

    df.to_sql(name=table_name, con=pg_conn, if_exists="append", method=postgres_upsert, index=False, dtype={"additional_data": JSONB})

    msg = f"Loaded {len(df)} records to {table_name} table."
    if context is None:
        print(msg)
    else:
        context.log.info(msg)
