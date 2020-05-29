from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, 
                               LoadFactOperator,
                               LoadDimensionOperator, 
                               DataQualityOperator,
                               CreateTablesOperator,
                               DropTablesOperator)
from helpers import SqlQueries

S3_BUCKET = 's3://udacity-dend/'
AWS_CREDENTIALS = S3Hook(aws_conn_id='aws_credentials').get_credentials()

AWS_KEY = AWS_CREDENTIALS.access_key
AWS_SECRET = AWS_CREDENTIALS.secret_key
LOG_DATA = S3_BUCKET + 'log_data'
LOG_JSONPATH = S3_BUCKET + 'log_json_path.json'
SONG_DATA = S3_BUCKET + 'song_data'
REDSHIFT_CONN_ID = "redshift"
TABLES = ['public.staging_events', 'public.staging_songs', 'public.songplays', 'public.users', 'public.songs', 'public.artists', 'public.times']

default_args = {
    'catchup': False,
    'Depends_on_past': False,
    'email_on_retry': False,
    'owner': 'udacity',
    'retries': 3,
    'retry_delay': 5,
    'start_date': datetime(2020, 5, 23),
    'max_active_runs': 1,
    'concurrency': 1
}

dag = DAG('udac_example_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = timedelta(hours = 1)
        )

start_operator = DummyOperator(task_id = 'Begin_execution', dag = dag)
    
stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    aws_key_id = AWS_KEY,
    aws_access_key = AWS_SECRET,
    json_file = LOG_JSONPATH,
    redshift_conn_id = REDSHIFT_CONN_ID,
    s3_source = LOG_DATA,
    table = "public.staging_events",
    dag = dag,
    provide_context = True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    aws_key_id = AWS_KEY,
    aws_access_key = AWS_SECRET,
    redshift_conn_id = REDSHIFT_CONN_ID,
    s3_source = SONG_DATA,
    table = "public.staging_songs",
    dag = dag,
    provide_context = True
)

create_tables = CreateTablesOperator(
    task_id = 'Create_tables',
    redshift_conn_id = REDSHIFT_CONN_ID,
    tables = TABLES,
    dag = dag,
    provide_context = True
)

load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    redshift_conn_id = REDSHIFT_CONN_ID,
    sql_statement = SqlQueries.songplay_table_insert,
    dag = dag,
    provide_context = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    redshift_conn_id = REDSHIFT_CONN_ID,
    delete_on_load = True,
    sql_statement = SqlQueries.user_table_insert,
    table = "public.users",
    dag = dag,
    provide_context = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    redshift_conn_id = REDSHIFT_CONN_ID,
    delete_on_load = True,
    sql_statement = SqlQueries.song_table_insert,
    table = "public.songs",
    dag = dag,
    provide_context = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    redshift_conn_id = REDSHIFT_CONN_ID,
    delete_on_load = True,
    sql_statement = SqlQueries.artist_table_insert,
    table = "public.artists",
    dag = dag,
    provide_context = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    redshift_conn_id = REDSHIFT_CONN_ID,
    delete_on_load = True,
    sql_statement = SqlQueries.time_table_insert,
    table = "public.times",
    dag = dag,
    provide_context = True
)

run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    redshift_conn_id = REDSHIFT_CONN_ID,
    tables = TABLES,
    dag = dag
)

end_operator = DummyOperator(task_id = 'Stop_execution',  dag=dag)

start_operator >> create_tables
create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_time_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_song_dimension_table]
[load_time_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_song_dimension_table, load_songplays_table] >> run_quality_checks
run_quality_checks >> end_operator