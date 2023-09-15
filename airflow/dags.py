from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from utils import *
from createtables import *
import constants as CS
from sql import SqlQueries

default_args = {
    "owner": "admin",
    "start_date": days_ago(0),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "catchup": False
}

dag = DAG(
    'process_song_plays_data',
    default_args = default_args,
    description = 'Load and transform data in Redshift with Airflow.'
)

start_operator = DummyOperator(task_id = "Begin_execution", dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    table = "public.staging_events",
    redshift_conn_id = CS.REDSHIFT_CONN_ID,
    aws_credentials_id = CS.AWS_CREDENTIALS_ID,
    s3_bucket = CS.S3_BUCKET,
    s3_key = "log_data",
    region = CS.REGION,
    create_table = staging_events_table_create,
    format = "json 's3://udacity-dend/log_json_path.json'",
    provide_context = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    table = "public.staging_songs",
    redshift_conn_id = CS.REDSHIFT_CONN_ID,
    aws_credentials_id = CS.AWS_CREDENTIALS_ID,
    s3_bucket = CS.S3_BUCKET,
    s3_key = "song_data",
    region = CS.REGION,
    create_table = staging_songs_table_create,
    format = "json 'auto'",
    provide_context = True
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    table = "public.songplays",
    redshift_conn_id = CS.REDSHIFT_CONN_ID,
    create_table = songplay_table_create,
    load_table = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag,
    table = "public.users",
    redshift_conn_id = CS.REDSHIFT_CONN_ID,
    create_table = user_table_create,
    load_table = SqlQueries.user_table_insert,
    truncateInsert = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    table = "public.songs",
    redshift_conn_id = CS.REDSHIFT_CONN_ID,
    create_table = song_table_create,
    load_table = SqlQueries.song_table_insert,
    truncateInsert = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    table = "public.artists",
    redshift_conn_id = CS.REDSHIFT_CONN_ID,
    create_table = artist_table_create,
    load_table = SqlQueries.artist_table_insert,
    truncateInsert = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    table = "public.dimTime",
    redshift_conn_id = CS.REDSHIFT_CONN_ID,
    create_table = time_table_create,
    load_table = SqlQueries.time_table_insert,
    truncateInsert = True
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
    redshift_conn_id = CS.REDSHIFT_CONN_ID,
    sql_tests = SqlQueries.tests,
    expected_results = SqlQueries.results
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator