"""
Marketo to Redshift

This files contains three dags:
    - A monthly backfill from Jan 1, 2013.
    - A daily backfill from Jan 1, 2018.
    - An ongoing hourly workflow.

Each DAG makes use of three custom operators:
    - RateLimitOperator
    https://github.com/airflow-plugins/rate_limit_plugin/blob/master/operators/rate_limit_operator.py
    - MarketoToS3Operator
    https://github.com/airflow-plugins/marketo_plugin/blob/master/operators/marketo_to_s3_operator.py#L19
    - S3ToRedshiftOperator
    https://github.com/airflow-plugins/redshift_plugin/blob/master/operators/s3_to_redshift_operator.py#L13

This ongoing DAG pulls the following Marketo objects:
    - Activities
    - Campaigns
    - Leads
    - Lead Lists
    - Programs

When backfilling, only the leads object is pulled. By default, it begins
pulling since Jan 1, 2013.
"""

from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (MarketoToS3Operator,
                               S3ToRedshiftOperator,
                               RateLimitOperator)
from MarketoPlugin.schemas._schema import schema

MARKETO_CONN_ID = ''
MARKETO_SCHEMA = ''
REDSHIFT_SCHEMA = ''
REDSHIFT_CONN_ID = ''
S3_CONN_ID = ''
S3_BUCKET = ''
RATE_LIMIT_THRESHOLD = 0.8
RATE_LIMIT_THRESHOLD_TYPE = 'percentage'

hourly_id = '{}_to_redshift_hourly'.format(MARKETO_CONN_ID)
daily_id = '{}_to_redshift_daily_backfill'.format(MARKETO_CONN_ID)
monthly_id = '{}_to_redshift_monthly_backfill'.format(MARKETO_CONN_ID)


def create_dag(dag_id,
               schedule,
               marketo_conn_id,
               redshift_conn_id,
               redshift_schema,
               s3_conn_id,
               s3_bucket,
               default_args,
               catchup=False):

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args,
              catchup=catchup)

    if 'backfill' in dag_id:
        endpoints = ['leads']
    else:
        endpoints = ['activities',
                     'campaigns',
                     'leads',
                     'programs',
                     'lead_lists']

    COPY_PARAMS = ["COMPUPDATE OFF",
                   "STATUPDATE OFF",
                   "JSON 'auto'",
                   "TIMEFORMAT 'auto'"
                   "TRUNCATECOLUMNS",
                   "region as 'us-east-1'"]

    with dag:
        d = DummyOperator(task_id='kick_off_dag')

        l = RateLimitOperator(task_id='check_rate_limit',
                              service='marketo',
                              service_conn_id=marketo_conn_id,
                              threshold=RATE_LIMIT_THRESHOLD,
                              threshold_type=RATE_LIMIT_THRESHOLD_TYPE)

        d >> l

        for endpoint in endpoints:

            MARKETO_SCHEMA = schema[endpoint]
            TABLE_NAME = 'mkto_{0}'.format(endpoint)

            S3_KEY = 'marketo/{0}/{1}_{2}.json'.format(redshift_schema,
                                                       endpoint,
                                                       "{{ ts_nodash }}")

            MARKETO_TASK_ID = 'get_{0}_marketo_data'.format(endpoint)
            REDSHIFT_TASK_ID = 'marketo_{0}_to_redshift'.format(endpoint)

            START_AT = "{{ execution_date.isoformat() }}"
            END_AT = "{{ next_execution_date.isoformat() }}"

            m = MarketoToS3Operator(task_id=MARKETO_TASK_ID,
                                    marketo_conn_id=marketo_conn_id,
                                    endpoint=endpoint,
                                    s3_conn_id=s3_conn_id,
                                    s3_bucket=s3_bucket,
                                    s3_key=S3_KEY,
                                    output_format='json',
                                    start_at=START_AT,
                                    end_at=END_AT)

            l >> m

            if endpoint != 'leads':
                r = S3ToRedshiftOperator(task_id=REDSHIFT_TASK_ID,
                                         s3_conn_id=s3_conn_id,
                                         s3_bucket=s3_bucket,
                                         s3_key=S3_KEY,
                                         load_type='rebuild',
                                         load_format='JSON',
                                         schema_location='local',
                                         origin_schema=MARKETO_SCHEMA,
                                         redshift_schema=redshift_schema,
                                         table=TABLE_NAME,
                                         copy_params=COPY_PARAMS,
                                         redshift_conn_id=redshift_conn_id)
                m >> r
            else:
                rl = S3ToRedshiftOperator(task_id=REDSHIFT_TASK_ID,
                                          s3_conn_id=s3_conn_id,
                                          s3_bucket=s3_bucket,
                                          s3_key=S3_KEY,
                                          load_type='upsert',
                                          load_format='JSON',
                                          schema_location='local',
                                          origin_schema=MARKETO_SCHEMA,
                                          redshift_schema=redshift_schema,
                                          table=TABLE_NAME,
                                          primary_key='id',
                                          copy_params=COPY_PARAMS,
                                          incremental_key='updated_at',
                                          redshift_conn_id=redshift_conn_id)

                m >> rl

    return dag


globals()[hourly_id] = create_dag(hourly_id,
                                  '@hourly',
                                  MARKETO_CONN_ID,
                                  REDSHIFT_CONN_ID,
                                  REDSHIFT_SCHEMA,
                                  S3_CONN_ID,
                                  S3_BUCKET,
                                  {'start_date': datetime(2018, 2, 13),
                                   'retries': 2,
                                   'retry_delay': timedelta(minutes=5),
                                   'email': [],
                                   'email_on_failure': True},
                                  catchup=True)

globals()[daily_id] = create_dag(daily_id,
                                 '@daily',
                                 MARKETO_CONN_ID,
                                 REDSHIFT_CONN_ID,
                                 REDSHIFT_SCHEMA,
                                 S3_CONN_ID,
                                 S3_BUCKET,
                                 {'start_date': datetime(2018, 1, 1),
                                  'end_date': datetime(2018, 2, 13),
                                  'retries': 2,
                                  'retry_delay': timedelta(minutes=5),
                                  'email': [],
                                  'email_on_failure': True},
                                 catchup=True)

globals()[monthly_id] = create_dag(monthly_id,
                                   '@monthly',
                                   MARKETO_CONN_ID,
                                   REDSHIFT_CONN_ID,
                                   REDSHIFT_SCHEMA,
                                   S3_CONN_ID,
                                   S3_BUCKET,
                                   {'start_date': datetime(2013, 1, 1),
                                    'end_date': datetime(2018, 1, 1),
                                    'retries': 2,
                                    'retry_delay': timedelta(minutes=5),
                                    'email': [],
                                    'email_on_failure': True},
                                   catchup=True)
