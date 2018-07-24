"""
Salesforce to Redshift

This files contains an ongoing hourly workflow.

Each DAG makes use of three custom operators:
    - SalesforceToS3Operator
    https://github.com/airflow-plugins/salesforce_plugin/blob/master/operators/salesforce_to_s3_operator.py#L60
    - S3ToRedshiftOperator
    https://github.com/airflow-plugins/redshift_plugin/blob/master/operators/s3_to_redshift_operator.py#L13

This ongoing DAG pulls the following Salesforce objects:
    - Account
    - Campaign
    - CampaignMember
    - Contact
    - Lead
    - Opportunity
    - OpportunityContactRole
    - OpportunityHistory
    - Task
    - User

The output from Salesforce will be formatted as newline delimited JSON (ndjson)
and will include """
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.salesforce_plugin import SalesforceToS3Operator
from airflow.operators import S3ToRedshiftOperator

SF_CONN_ID = ''
S3_CONN_ID = ''
S3_BUCKET = ''
REDSHIFT_CONN_ID = ''
REDSHIFT_SCHEMA_NAME = ''
ORIGIN_SCHEMA = ''
SCHEMA_LOCATION = ''

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 8, 29),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('salesforce_to_redshift',
          default_args=default_args,
          schedule_interval='@hourly',
          catchup=False)

tables = [{'name': 'Account',
           'load_type': 'Upsert'},
          {'name': 'Campaign',
           'load_type': 'Upsert'},
          {'name': 'CampaignMember',
           'load_type': 'Upsert'},
          {'name': 'Contact',
           'load_type': 'Upsert'},
          {'name': 'Lead',
           'load_type': 'Upsert'},
          {'name': 'Opportunity',
           'load_type': 'Upsert'},
          {'name': 'OpportunityContactRole',
           'load_type': 'Upsert'},
          {'name': 'OpportunityHistory',
           'load_type': 'Upsert'},
          {'name': 'Task',
           'load_type': 'Upsert'},
          {'name': 'User',
           'load_type': 'Upsert'}]

COPY_PARAMS = ["JSON 'auto'",
               "TRUNCATECOLUMNS",
               "region as 'us-east-1'"]

kick_off_dag = DummyOperator(task_id='kick_off_dag', dag=dag)

for table in tables:
    S3_KEY = 'salesforce/{}/{}.json'.format('{{ execution_date }}',
                                            table['name'].lower())

    salesforce_to_s3 = SalesforceToS3Operator(task_id='{0}_to_S3'.format(table['name']),
                                              sf_conn_id=SF_CONN_ID,
                                              sf_obj=table,
                                              fmt='ndjson',
                                              s3_conn_id=S3_CONN_ID,
                                              s3_bucket=S3_BUCKET,
                                              record_time_added=True,
                                              coerce_to_timestamp=True,
                                              dag=dag)

    s3_to_redshift = S3ToRedshiftOperator(task_id='{0}_to_Redshift'.format(table['name']),
                                          redshift_conn_id=REDSHIFT_CONN_ID,
                                          redshift_schema=REDSHIFT_SCHEMA_NAME,
                                          table=table,
                                          s3_conn_id=S3_CONN_ID,
                                          s3_bucket=S3_BUCKET,
                                          s3_key=S3_KEY,
                                          origin_schema=ORIGIN_SCHEMA,
                                          copy_params=COPY_PARAMS,
                                          schema_location=SCHEMA_LOCATION,
                                          load_type=table['load_type'],
                                          dag=dag)

    kick_off_dag >> salesforce_to_s3 >> s3_to_redshift
