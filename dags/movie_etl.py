import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

dag = DAG(
    dag_id = 'movie_etl',
    start_date = airflow.utils.dates.days_ago(1),
    schedule_interval = None, #set to same schedule as cload watch trigger
    template_searchpath = '/opt/SQL',
    catchup = False,
)


#trigger_first_lambda = LambdaInvokeFunctionOperator(
#    task_id = 'trigger_extract_lambda',
#    function_name = 'movie_extract_pipe',
#    aws_conn_id = 'aws',
#    region_name= 'us-east-1',
#    dag = dag
#)

check_s3_upload_1 = S3KeySensor(
    task_id='check_s3_upload_1',
    bucket_key='s3://movies-etl-project-at/raw_data/to_process/*.json',
    wildcard_match=True,
    aws_conn_id='aws',
    timeout=60 * 60,  # wait for up to 1 hour
    poke_interval=60,  # check every 60 seconds
    dag=dag,
)

trigger_second_lambda = LambdaInvokeFunctionOperator(
    task_id = 'trigger_transformation_lambda',
    function_name = 'movie_transform_pipe',
    aws_conn_id = 'aws',
    region_name= 'us-east-1',
    dag = dag
)

check_s3_upload_2 = S3KeySensor(
    task_id='check_s3_upload_2',
    bucket_key='s3://movies-etl-project-at/raw_data/processed/*.json',
    wildcard_match=True,
    aws_conn_id='aws',
    timeout=60 * 60,  # wait for up to 1 hour
    poke_interval=60,  # check every 60 seconds
    dag=dag,
)

snowflake_load_stage_tables = SnowflakeOperator(
    task_id = 'load_stage_tables_snowflake',
    snowflake_conn_id = 'snowflake_moviedb',
    sql = 'load_data.sql',
    role = 'ACCOUNTADMIN',
    schema = 'stage'
)

snowflake_update_target_tables = SnowflakeOperator(
    task_id = 'update_target_tables_snowflake',
    snowflake_conn_id = 'snowflake_moviedb',
    sql = 'update_tables.sql',
    role = 'ACCOUNTADMIN',
    schema = 'prod'
)

check_s3_upload_1 >> trigger_second_lambda >> check_s3_upload_2 >> snowflake_load_stage_tables >> snowflake_update_target_tables

