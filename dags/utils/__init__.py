import pandas as pd
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from contextlib import closing


def pandas_to_snowflake(
    df: pd.DataFrame,
    db: str,
    schema: str,
    table_name: str,
    chunk_size: int = 10_000,
    snowflake_conn_id: str = "sf",
):
    """
    Upload pandas dataframe to Snowflake.
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    snowflake_engine = snowflake_hook.get_sqlalchemy_engine()

    with snowflake_engine.begin() as connection:
        df.to_sql(
            chunksize=chunk_size,
            con=connection,
            index=False,
            method="multi",
            name=table_name,
            schema=f"{db}.{schema}",
        )
        snowflake_engine.dispose()


def execute_snowflake(
    snowflake_conn_id,
    sql,
    fetch=True,
    pre_query=None,
    with_cursor=False,
):
    """
    Execute snowflake query.
    """
    hook_connection = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

    with closing(hook_connection.get_conn()) as conn:
        with closing(conn.cursor()) as cur:
            if pre_query:
                cur.execute(pre_query)

            cur.execute(sql)
            if fetch:
                res = cur.fetchall()
                if with_cursor:
                    return (res, cur)
                else:
                    return res


def snowflake_to_pandas(
    query,
    snowflake_conn_id,
    dtype=None,
):
    """
    Convert snowflake list to pandas dataframe.
    """
    result, cursor = execute_snowflake(
        sql=query,
        snowflake_conn_id=snowflake_conn_id,
        with_cursor=True,
    )

    headers = list(map(lambda t: t[0], cursor.description))

    return pd.DataFrame(
        result,
        columns=headers,
        dtype=dtype,
    )
