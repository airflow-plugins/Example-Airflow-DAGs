"""
Google Analytics to Redshift

This file contains one ongoing hourly DAG.

This DAG makes use of two custom operators:
    - GoogleAnalyticsToS3Operator
    https://github.com/airflow-plugins/google_analytics_plugin/blob/master/operators/google_analytics_reporting_to_s3_operator.py#L11
    - S3ToRedshiftOperator
    https://github.com/airflow-plugins/redshift_plugin/blob/master/operators/s3_to_redshift.py#L13

This DAG generates a report using v4 of the Google Analytics Core Reporting
API. The dimensions and metrics are as follows. Note that while these can be
modified, a maximum of 10 metrics and 7 dimensions can be requested at once.

METRICS
    - pageView
    - bounces
    - users
    - newUsers
    - goal1starts
    - goal1completions


DIMENSIONS
    - dateHourMinute
    - keyword
    - referralPath
    - campaign
    - sourceMedium

Not all metrics and dimensions are compatible with each other. When forming
the request, please refer to the official Google Analytics API Reference docs:
https://developers.google.com/analytics/devguides/reporting/core/dimsmets
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from GoogleAnalyticsPlugin.schemas.google_analytics_schemas import google_analytics_reporting_schema
from airflow.operators import (GoogleAnalyticsReportingToS3Operator,
                               S3ToRedshiftOperator)

S3_CONN_ID = ''
S3_BUCKET = ''
GOOGLE_ANALYTICS_CONN_ID = ''
REDSHIFT_CONN_ID = ''
REDSHIFT_SCHEMA = ''

# Google Analytics has a "lookback window" that defaults to 30 days.
# During this period, metrics are in flux as users return to the property
# and complete various actions and conversion goals.
# https://support.google.com/analytics/answer/1665189?hl=en

# The period set as the LOOKBACK_WINDOW will be dropped and replaced during
# each run of this workflow.

LOOKBACK_WINDOW = 30

# NOTE: While GA supports relative input dates, it is not advisable to use
# these in case older workflows need to be re-run.

# https://developers.google.com/analytics/devguides/reporting/core/v4/basics
SINCE = "{{{{ macros.ds_add(ds, -{0}) }}}}".format(str(LOOKBACK_WINDOW))
UNTIL = "{{ ds }}"

view_ids = []

# https://developers.google.com/analytics/devguides/reporting/core/v3/reference#sampling
SAMPLING_LEVEL = None

# https://developers.google.com/analytics/devguides/reporting/core/v3/reference#includeEmptyRows
INCLUDE_EMPTY_ROWS = False

PAGE_SIZE = 1000

# NOTE: Not all metrics and dimensions are available together. It is
# advisable to test with the GA explorer before deploying.
# https://developers.google.com/analytics/devguides/reporting/core/dimsmets

METRICS = [{'expression': 'ga:pageViews'},
           {'expression': 'ga:bounces'},
           {'expression': 'ga:users'},
           {'expression': 'ga:newUsers'},
           {'expression': 'ga:goal1starts'},
           {'expression': 'ga:goal1completions'}]


DIMENSIONS = [{'name': 'ga:dateHourMinute'},
              {'name': 'ga:keyword'},
              {'name': 'ga:referralPath'},
              {'name': 'ga:campaign'},
              {'name': 'ga:sourceMedium'}]

# The specified TIMEFORMAT is based on the ga:dateHourMinute dimension.
# If using ga:date or ga:dateHour, this format will need to adjust accordingly.
COPY_PARAMS = ["COMPUPDATE OFF",
               "STATUPDATE OFF",
               "JSON 'auto'",
               "TIMEFORMAT 'YYYYMMDDHHMI'"
               "TRUNCATECOLUMNS",
               "region as 'us-east-1'"]

# Primary and Incremental Keys are set to same value as no other reliable
# primary_key can found. This will result in all records with matching values of
# dateHourMinute to be deleted and new records inserted for the period of time
# covered by the lookback window. Timestamps matching records greater than
# the lookback window from the current data will not be pulled again and
# therefore not replaced.

PRIMARY_KEY = 'datehourminute'
INCREMENTAL_KEY = 'datehourminute'

default_args = {'start_date': datetime(2018, 2, 22),
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'email': [],
                'email_on_failure': True}

dag = DAG('{}_to_redshift_hourly'.format(GOOGLE_ANALYTICS_CONN_ID),
          schedule_interval='@hourly',
          default_args=default_args,
          catchup=False)

with dag:
    d = DummyOperator(task_id='kick_off_dag')

    for view_id in view_ids:
        S3_KEY = 'google_analytics/{0}/{1}_{2}_{3}.json'.format(REDSHIFT_SCHEMA,
                                                                GOOGLE_ANALYTICS_CONN_ID,
                                                                view_id,
                                                                "{{ ts_nodash }}")

        g = GoogleAnalyticsReportingToS3Operator(task_id='get_google_analytics_data',
                                                 google_analytics_conn_id=GOOGLE_ANALYTICS_CONN_ID,
                                                 view_id=view_id,
                                                 since=SINCE,
                                                 until=UNTIL,
                                                 sampling_level=SAMPLING_LEVEL,
                                                 dimensions=DIMENSIONS,
                                                 metrics=METRICS,
                                                 page_size=PAGE_SIZE,
                                                 include_empty_rows=INCLUDE_EMPTY_ROWS,
                                                 s3_conn_id=S3_CONN_ID,
                                                 s3_bucket=S3_BUCKET,
                                                 s3_key=S3_KEY
                                                 )

        redshift = S3ToRedshiftOperator(task_id='sink_to_redshift',
                                        redshift_conn_id=REDSHIFT_CONN_ID,
                                        redshift_schema=REDSHIFT_SCHEMA,
                                        table='report_{}'.format(view_id),
                                        s3_conn_id=S3_CONN_ID,
                                        s3_bucket=S3_BUCKET,
                                        s3_key=S3_KEY,
                                        origin_schema=google_analytics_reporting_schema,
                                        schema_location='local',
                                        copy_params=COPY_PARAMS,
                                        load_type='upsert',
                                        primary_key=PRIMARY_KEY,
                                        incremental_key=INCREMENTAL_KEY
                                        )

        d >> g >> redshift
