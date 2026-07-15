'''
This will perform data modification operations on the data warehouse, such as inserting, updating or deleting records.
'''

import logging

logger = logging.getLogger(__name__)
table = 'yt_api' 

def insert_rows(cur, conn, schema, row):
    """
    Insert a row into the specified table in the Postgres database.

    Args:
        cur: A cursor object for executing queries.
        conn: A connection object to the Postgres database.
        schema: The name of the schema where the table resides.
        row: A tuple containing the values to be inserted into the table.
    """


    try:
        
        if schema == 'staging':
            
            video_id = 'video_id'

            cur.execute(f"""
                INSERT INTO {schema}.{table} (Video_ID, Video_Title, Upload_Date, Duration, Video_Views, Likes_Count, Comments_Count)
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)
            """, row)

        else:

            video_id = 'video_id'

            cur.execute(f"""
                INSERT INTO {schema}.{table} (Video_ID, Video_Title, Upload_Date, Duration, Video_Type, Video_Views, Likes_Count, Comments_Count)
                VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s)
            """, row)

        conn.commit()  # Commit the transaction to save changes

        logger.info(f"Row inserted successfully into {schema}.{table}: {row}")

    except Exception as e:
        logger.error(f"Error occurred while inserting row: {row[video_id]}")
        conn.rollback()  # Rollback the transaction in case of an error
        raise e

def update_rows(cur, conn, schema, row):
    """
    Update a row in the specified table in the Postgres database.

    Args:
        cur: A cursor object for executing queries.
        conn: A connection object to the Postgres database.
        schema: The name of the schema where the table resides.
        row: A tuple containing the values to be updated in the table.
    """
    try:
        if schema == 'staging':
            video_id = 'video_id'

            cur.execute(f"""
                UPDATE {schema}.{table}
                SET Video_Title = %(title)s,
                    Upload_Date = %(publishedAt)s,
                    Duration = %(duration)s,
                    Video_Views = %(viewCount)s,
                    Likes_Count = %(likeCount)s,
                    Comments_Count = %(commentCount)s
                WHERE Video_ID = %(video_id)s
            """, row)

        else:
            video_id = 'video_id'

            cur.execute(f"""
                UPDATE {schema}.{table}
                SET Video_Title = %(Video_Title)s,
                    Upload_Date = %(Upload_Date)s,
                    Duration = %(Duration)s,
                    Video_Type = %(Video_Type)s,
                    Video_Views = %(Video_Views)s,
                    Likes_Count = %(Likes_Count)s,
                    Comments_Count = %(Comments_Count)s
                WHERE Video_ID = %(Video_ID)s
            """, row)

        conn.commit()  # Commit the transaction to save changes

        logger.info(f"Row updated successfully in {schema}.{table}: {row}")

    except Exception as e:
        logger.error(f"Error occurred while updating row: {row[video_id]}")
        conn.rollback()  # Rollback the transaction in case of an error
        raise e