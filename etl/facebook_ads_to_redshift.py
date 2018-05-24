"""
Facebook Ads to Redshift

This file contains one ongoing daily DAG.

This DAG makes use of two custom operators:
    - FacebookAdsInsightsToS3Operator
    https://github.com/airflow-plugins/facebook_ads_plugin/blob/master/operators/facebook_ads_to_s3_operator.py#L10
    - S3ToRedshiftOperator
    https://github.com/airflow-plugins/redshift_plugin/blob/master/operators/s3_to_redshift_operator.py#L13

This DAG creates four breakdown reports:
    - age_gender
    - device_platform
    - region_country
    - no_breakdown

The standard fields included in each report are as follows:
    - account_id
    - ad_id
    - adset_id
    - ad_name
    - adset_name
    - campaign_id
    - date_start
    - date_stop
    - campaign_name
    - clicks
    - cpc
    - cpm
    - cpp
    - ctr
    - impressions
    - objective
    - reach
    - social_clicks
    - social_impressions
    - social_spend
    - spend
    - total_unique_actions

In addition these standard fields, custom fields can also be specified.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import FacebookAdsInsightsToS3Operator, S3ToRedshiftOperator

time_string = '{{ ts_nodash }}'
FACEBOOK_CONN_ID = ''
ACCOUNT_ID = ''
S3_BUCKET = ''
S3_CONN_ID = ''
REDSHIFT_CONN_ID = ''
REDSHIFT_SCHEMA = ''

default_args = {
    'start_date': datetime(2016, 1, 1, 0, 0),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'depends_on_past': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('facebook_ads_to_redshift',
          schedule_interval='@daily',
          default_args=default_args,
          catchup=True)

COPY_PARAMS = ["COMPUPDATE OFF",
               "STATUPDATE OFF",
               "JSON 'auto'",
               "TIMEFORMAT 'auto'"
               "TRUNCATECOLUMNS",
               "region as 'us-east-1'"]

execution_date = '{{ execution_date }}'
next_execution_date = '{{ next_execution_date }}'


breakdowns = [
    {
        'name': 'age_gender',
        'fields': [
            {'name': 'age', 'type': 'varchar(64)'},
            {'name': 'gender', 'type': 'varchar(64)'}
        ]
    },
    {
        'name': 'device_platform',
        'fields': [
            {'name': 'device_platform', 'type': 'varchar(64)'}
        ]
    },
    {
        'name': 'region_country',
        'fields': [
            {'name': 'region', 'type': 'varchar(128)'},
            {'name': 'country', 'type': 'varchar(128)'}
        ]
    },
    {
        'name': 'no_breakdown',
        'fields': []
    }
]

fields = [
    {'name': 'account_id', 'type': 'varchar(64)'},
    {'name': 'ad_id', 'type': 'varchar(64)'},
    {'name': 'adset_id', 'type': 'varchar(64)'},
    {'name': 'campaign_id', 'type': 'varchar(64)'},
    {'name': 'date_start', 'type': 'date'},
    {'name': 'date_stop', 'type': 'date'},
    {'name': 'ad_name', 'type': 'varchar(255)'},
    {'name': 'adset_name', 'type': 'varchar(255)'},
    {'name': 'campaign_name', 'type': 'varchar(255)'},
    {'name': 'clicks', 'type': 'int(11)'},
    {'name': 'cpc', 'type': 'decimal(20,6)'},
    {'name': 'cpm', 'type': 'decimal(20,6)'},
    {'name': 'cpp', 'type': 'decimal(20,6)'},
    {'name': 'ctr', 'type': 'decimal(20,6)'},
    {'name': 'impressions', 'type': 'int(11)'},
    {'name': 'objective', 'type': 'varchar(255)'},
    {'name': 'reach', 'type': 'int(11)'},
    {'name': 'social_clicks', 'type': 'int(11)'},
    {'name': 'social_impressions', 'type': 'int(11)'},
    {'name': 'social_spend', 'type': 'decimal(20,6)'},
    {'name': 'spend', 'type': 'decimal(20,6)'},
    {'name': 'total_unique_actions', 'type': 'int(11)'}
]

field_names = [field['name'] for field in fields]

# Add any custom fields after building insight api field_names
fields.extend([{'name': 'example', 'type': 'text'}])

start = DummyOperator(
    task_id='start',
    dag=dag
)

for breakdown in breakdowns:

    breakdown_fields = [field['name'] for field in breakdown['fields']]

    S3_KEY = 'facebook_insights/{}_{}'.format(breakdown['name'], time_string)

    facebook_ads = FacebookAdsInsightsToS3Operator(
        task_id='facebook_ads_{}_to_s3'.format(breakdown['name']),
        facebook_conn_id=FACEBOOK_CONN_ID,
        s3_conn_id=S3_CONN_ID,
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        account_ids=ACCOUNT_ID,
        insight_fields=field_names,
        breakdowns=breakdown_fields,
        since=execution_date,
        until=next_execution_date,
        time_increment=1,
        level='ad',
        limit=200,
        dag=dag
    )

    # Append breakdown fields (primary keys) after
    # primary keys which are in every workflow
    output_table_fields = list(fields)
    output_table_fields = output_table_fields[:4] + breakdown['fields'] + output_table_fields[4:]

    primary_key = ['ad_id',
                   'adset_id',
                   'campaign_id',
                   'account_id',
                   'date_start']

    primary_key.extend(breakdown_fields)

    s3_to_redshift = S3ToRedshiftOperator(
        task_id='s3_{}_to_redshift'.format(breakdown['name']),
        s3_conn_id=S3_CONN_ID,
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        redshift_conn_id=REDSHIFT_CONN_ID,
        redshift_schema=REDSHIFT_SCHEMA,
        copy_params=COPY_PARAMS,
        table=breakdown['name'],
        origin_schema=output_table_fields,
        schema_location='local',
        primary_key=primary_key,
        load_type='upsert',
        dag=dag
    )

    start >> facebook_ads >> s3_to_redshift
