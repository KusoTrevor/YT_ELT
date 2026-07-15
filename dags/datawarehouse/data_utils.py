from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyscopg2.extras import RealDictCursor

def get_conn_cursor():
    """
    Get a connection and cursor to the Postgres database using Airflow's PostgresHook.

    Returns:
        conn: A connection object to the Postgres database.
        cursor: A cursor object for executing queries.
    """
    hook = PostgresHook(postgres_conn_id='postgres_db_yt_elt', database='elt_db')
    conn = hook.get_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cursor

def close_conn_cursor(conn, cursor):
    """
    Close the cursor and connection to the Postgres database.

    Args:
        conn: A connection object to the Postgres database.
        cursor: A cursor object for executing queries.
    """
    cursor.close()  # Close the cursor to avoid resource leaks
    conn.close()  # Close the connection to avoid resource leaks