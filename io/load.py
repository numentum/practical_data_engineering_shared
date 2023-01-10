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
    if pg_conn is None:
        assert os.getenv("PG_CONN") is not None, "PG_CONN environment variable is not set."
        pg_conn = os.getenv("PG_CONN")

    df.to_sql(name=table_name, con=pg_conn, if_exists="append", method=postgres_upsert, index=False, dtype={"additional_data": JSONB})

    msg = f"Loaded {len(df)} records to {table_name} table."
    if context is None:
        print(msg)
    else:
        context.log.info(msg)
