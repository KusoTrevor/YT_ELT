from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = 'yt_api'

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

def create_schema_if_not_exists(schema_name):
    """
    Create a schema in the Postgres database if it does not already exist.

    Args:
        cursor: A cursor object for executing queries.
        schema_name: The name of the schema to create.
    """

    conn, cursor = get_conn_cursor()

    schema_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"

    cursor.execute(schema_query)  # Execute the query to create the schema if it doesn't exist

    conn.commit()  # Commit the transaction to save changes

    close_conn_cursor(conn, cursor)  # Close the connection and cursor to avoid resource leaks

def create_table_if_not_exists(schema_name, table_name):

    conn, cursor = get_conn_cursor()

    if schema_name == 'staging':
        table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT   
        );
        """
    else:
        table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                    "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                    "Video_Title" TEXT NOT NULL,
                    "Upload_Date" TIMESTAMP NOT NULL,
                    "Duration" VARCHAR(20) NOT NULL,
                    "Video_Views" INT,
                    "Likes_Count" INT,
                    "Comments_Count" INT   
        );
        """

    cursor.execute()

    conn.commit()  # Commit the transaction to save changes

    close_conn_cursor(conn, cursor)  # Close the connection and cursor to avoid resource leaks

def get_video_ids_from_table(schema_name, cursor    ):
    """
    Retrieve all video IDs from the specified table in the Postgres database.

    Args:
        schema_name: The name of the schema where the table is located.
        table_name: The name of the table from which to retrieve video IDs.

    Returns:
        A list of video IDs.
    """
    conn, cursor = get_conn_cursor()

    video_ids = []
    try:
        cursor.execute(f"SELECT Video_ID FROM {schema_name}.{table_name};")
        video_ids = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error occurred while fetching video IDs: {e}")
    finally:
        close_conn_cursor(conn, cursor)

    return video_ids