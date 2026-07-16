from airflow import DAG
import pendulum

from datetime import timedelta, datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json
from datawarehouse.dwh import staging_table, core_table

# define the local timezone
local_tz = pendulum.timezone("America/Los_Angeles")

# define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['trevorsayle@gmail.com'],
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
    'dagrun_timeout': timedelta(minutes=60),
    'start_date': datetime(2025, 1, 1, tzinfo=local_tz),
    # 'end_date': datetime(2025, 12, 31, tzinfo=local_tz),
}

with DAG(
    dag_id='produce_yt_video_json',
    default_args=default_args,
    description='A DAG to extract, transform, and load YouTube video statistics for MrBeast channel via json file',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    catchup=False, # do not catchup on missed runs
    tags=['youtube', 'video_stats'],
) as dag:

    # Define the tasks in the DAG
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extracted_data)

    # trigger_update_db = TriggerDagRunOperator(
    #     task_id="trigger_update_db",
    #     trigger_dag_id="update_db",
    # )

    # Define dependencies
    playlist_id >> video_ids >> extracted_data >> save_to_json_task# >> trigger_update_db

# DAG 2: update_db
with DAG(
    dag_id="update_db",
    default_args=default_args,
    description="DAG to process JSON file and insert data into both staging and core schemas",
    catchup=False,
    schedule_interval='0 1 * * *',  # Run daily at 1 AM
) as dag_update:

    # Define tasks
    update_staging = staging_table()
    update_core = core_table()

    # trigger_data_quality = TriggerDagRunOperator(
    #     task_id="trigger_data_quality",
    #     trigger_dag_id="data_quality",
    # )

    # Define dependencies
    update_staging >> update_core# >> trigger_data_quality

# # DAG 3: data_quality
# with DAG(
#     dag_id="data_quality",
#     default_args=default_args,
#     description="DAG to check the data quality on both layers in the database",
#     catchup=False,
#     schedule=None,
# ) as dag_quality:

#     # Define tasks
#     soda_validate_staging = yt_elt_data_quality(staging_schema)
#     soda_validate_core = yt_elt_data_quality(core_schema)

#     # Define dependencies
#     soda_validate_staging >> soda_validate_core