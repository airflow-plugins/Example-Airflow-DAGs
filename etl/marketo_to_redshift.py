from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (MarketoToS3Operator,
                               S3ToRedshiftOperator,
                               RateLimitOperator)
from MarketoPlugin.schemas._schema import schema


"""
Marketo to Redshift

This files contains three dags:
    - A monthly backfill from Jan 1, 2013.
    - A monthly backfill from Jan 1, 2018.
    - An ongoing hourly workflow.

Each DAG makes use of three custom operators:
    - RateLimitOperator
    - MarketoToS3Operator
    - S3ToRedshiftOperator

This ongoing DAG pulls the following Marketo objects:
    - Activities
    - Campaigns
    - Leads
    - Lead Lists
    - Programs

When backfilling, only the leads object is pulled. By default, it begins
pulling since Jan 1, 2013.
"""

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

            m = MarketoToS3Operator(task_id='get_{0}_marketo_data'.format(endpoint),
                                    marketo_conn_id=marketo_conn_id,
                                    endpoint=endpoint,
                                    s3_conn_id=S3_CONN_ID,
                                    s3_bucket=S3_BUCKET,
                                    s3_key=S3_KEY,
                                    output_format='json',
                                    start_at="{{ execution_date.isoformat() }}",
                                    end_at="{{ next_execution_date.isoformat() }}")

            l >> m

            if endpoint != 'leads':
                r = S3ToRedshiftOperator(task_id='marketo_{0}_to_redshift'.format(endpoint),
                                         s3_conn_id=S3_CONN_ID,
                                         s3_bucket=S3_BUCKET,
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
                rl = S3ToRedshiftOperator(task_id='marketo_{0}_to_redshift'.format(endpoint),
                                          s3_conn_id=S3_CONN_ID,
                                          s3_bucket=S3_BUCKET,
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
                                  {'start_date': datetime(2018, 2, 13),
                                   'retries': 2,
                                   'retry_delay': timedelta(minutes=5),
                                   'email': [
                                   'support+astronomer@calibermind.com',
                                   'oren@calibermind.com',
                                   'nic@calibermind.com',
                                   'tony@calibermind.com',
                                   'l5t3o4a9m9q9v1w9@astronomerteam.slack.com'
                                   ],
                                   'email_on_failure': True})

globals()[daily_id] = create_dag(daily_id,
                                 '@daily',
                                 MARKETO_CONN_ID,
                                 REDSHIFT_CONN_ID,
                                 REDSHIFT_SCHEMA,
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
                                   {'start_date': datetime(2013, 1, 1),
                                    'end_date': datetime(2018, 1, 1),
                                    'retries': 2,
                                    'retry_delay': timedelta(minutes=5),
                                    'email': [],
                                    'email_on_failure': True},
                                   catchup=True)
