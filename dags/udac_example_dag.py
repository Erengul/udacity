from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# setting default dag properties
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 2, 28),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# setting special dag properties
dag = DAG('udac_capstone_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# loading staging tables
stage_listing_to_redshift = StageToRedshiftOperator(
    task_id='Stage_listing',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    target_table='public.staging_listings',
    s3_bucket='airbnb-review',
    s3_key='listings',
    region='us-west-2',
    delimiter=','
)

stage_review_to_redshift = StageToRedshiftOperator(
    task_id='Stage_review',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    target_table='public.staging_reviews',
    s3_bucket='airbnb-review',
    s3_key='reviews',
    region='us-west-2',
    delimiter=';'
)


load_listing_table = LoadFactOperator(
    task_id='Load_listing_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='listing',
    load_sql=SqlQueries.listings_table_insert
)

# loading fact tables
load_review_table = LoadFactOperator(
    task_id='Load_review_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='reviews',
    load_sql=SqlQueries.reviews_table_insert
)

load_host_dimension_table = LoadDimensionOperator(
    task_id='Load_host_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='hosts',
    load_sql=SqlQueries.hosts_table_insert
)

# loading dimension tables
load_location_dimension_table = LoadDimensionOperator(
    task_id='Load_location_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='locations',
    load_sql=SqlQueries.locations_table_insert
)

load_reviewer_dimension_table = LoadDimensionOperator(
    task_id='Load_reviewer_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='reviewers',
    load_sql=SqlQueries.reviewers_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    load_sql=SqlQueries.time_table_insert
)

# setting data quality checks
# 1. check null primary keys
# 2. check duplicated values for primary keys on dimension tables
dq_checks=[{'check_sql': "select count(*) from reviewers WHERE reviewer_id is null", 'expected_result': 0},
           {'check_sql': "select count(*) FROM hosts WHERE host_id is null", 'expected_result': 0},
           {'check_sql': "select count(*) FROM locations WHERE latitude is null or longitude is null", 'expected_result': 0},
           {'check_sql': "select count(*) from(select host_id, count(*) from hosts group by host_id having count(*) > 1)", 'expected_result': 0},
           {'check_sql': "select count(*) from(select latitude, longitude, count(*) from locations group by latitude, longitude having count(*) > 1)", 'expected_result': 0},
           {'check_sql': "select count(*) from(select reviewer_id, count(*) from reviewers group by reviewer_id having count(*) > 1)", 'expected_result': 0}]


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks = dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# setting etl process
start_operator >> stage_listing_to_redshift >> load_listing_table
start_operator >> stage_review_to_redshift >> load_review_table
stage_listing_to_redshift >> load_review_table 
load_listing_table >> load_host_dimension_table >> run_quality_checks
load_listing_table >> load_location_dimension_table >> run_quality_checks
load_review_table >> load_reviewer_dimension_table >> run_quality_checks
load_review_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
