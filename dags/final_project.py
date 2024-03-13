from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements as sql


default_args = {
    'owner': 'Davi Martin',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'email': ['davimlmart@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    max_active_runs=1,
    catchup=False
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="udacity-davimlmart-data-pipeline",
        s3_key="log-data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
        json_format="s3://udacity-davimlmart-data-pipeline/log_json_path.json",
        create_table=sql.SqlQueries.staging_events_create
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="udacity-davimlmart-data-pipeline",
        s3_key="song-data/A/A/",
        create_table=sql.SqlQueries.staging_songs_create
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="songplays",
        create_table=sql.SqlQueries.songplay_table_create,
        insert_query=sql.SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="users",
        create_table=sql.SqlQueries.user_table_create,
        insert_query=sql.SqlQueries.user_table_insert,
        mode="overwrite"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="songs",
        create_table=sql.SqlQueries.song_table_create,
        insert_query=sql.SqlQueries.song_table_insert,
        mode="overwrite"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="artists",
        create_table=sql.SqlQueries.artist_table_create,
        insert_query=sql.SqlQueries.artist_table_insert,
        mode="overwrite"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="time",
        create_table=sql.SqlQueries.time_table_create,
        insert_query=sql.SqlQueries.time_table_insert,
        mode="overwrite"
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables_and_columns= [
            {'table': 'staging_events', 'columns': ['sessionId', 'ts']},
            {'table': 'staging_songs', 'columns': ['song_id', 'artist_id']},
            {'table': 'songplays', 'columns': ['songplay_id', 'user_id', 'song_id', 'artist_id', 'session_id']},
            {'table': 'users', 'columns': ['user_id', 'first_name', 'last_name', 'gender', 'level']},
            {'table': 'songs', 'columns': ['song_id', 'title', 'artist_id']},
            {'table': 'artists', 'columns': ['artist_id', 'artist_name']},
            {'table': 'time', 'columns': ['start_time', 'hour', 'day', 'week', 'year', 'month', 'weekday']},
        ]
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] 
    load_songplays_table << [stage_events_to_redshift, stage_songs_to_redshift]  
    load_songplays_table >> [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
    run_quality_checks << [load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table]
    run_quality_checks >> end_operator
    
final_project_dag = final_project()