"""
An example DAG designed to:
1) Access an IMAP Server
2) Search the inbox for an email with a specific subject
3) Pull in the csv attachments of the email and store them in S3
4) Load these files into Redshift.

This files contains one dag:
    - An ongoing daily workflow.

Each DAG makes use of two custom operators:
    - IMAPToS3Operator
    https://github.com/airflow-plugins/imap_plugin/blob/master/operators/imap_to_s3_operator.py#L13
    - S3ToRedshiftOperator
    https://github.com/airflow-plugins/redshift_plugin/blob/master/operators/s3_to_redshift.py#L13

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import ImapToS3Operator
from airflow.operators import S3ToRedshiftOperator

IMAP_CONN_ID = ''
IMAP_EMAIL = ''

S3_CONN_ID = ''
S3_BUCKET = ''

REDSHIFT_SCHEMA = ''
REDSHIFT_CONN_ID = ''

TIME = '{{ ts_nodash }}'


email_workflows = [
    {
        'id': 'transaction',
        'name': 'Transaction',
        'fields': [
            {'name': "event_code", 'type': "varchar(256)"},
            {'name': "customer_id", 'type': "varchar(256)"},
            {'name': "date", 'type': "timestamp"},
            {'name': "code", 'type': "varchar(256)"},
            {'name': "name", 'type': "varchar(256)"},
            {'name': "class_code", 'type': "varchar(256)"},
            {'name': "price", 'type': "varchar(256)"},
            {'name': "order_qty", 'type': "varchar(256)"},
        ]
    },
    {
        'id': 'customer',
        'name': 'Customers',
        'fields': [
            {'name': "customer_id", 'type': "varchar(256)"},
            {'name': "full_name", 'type': "varchar(256)"},
            {'name': "mail_addr1", 'type': "varchar(256)"},
            {'name': "mail_addr2", 'type': "varchar(256)"},
            {'name': "mail_city", 'type': "varchar(256)"},
            {'name': "mail_state", 'type': "varchar(256)"},
            {'name': "mail_zip", 'type': "varchar(256)"},
            {'name': "mail_country", 'type': "varchar(256)"},
            {'name': "bill_addr1", 'type': "varchar(256)"},
            {'name': "bill_addr2", 'type': "varchar(256)"},
            {'name': "bill_state", 'type': "varchar(256)"},
            {'name': "bill_city", 'type': "varchar(256)"},
            {'name': "bill_zip", 'type': "varchar(256)"},
            {'name': "bill_country", 'type': "varchar(256)"},
            {'name': "bill_name", 'type': "varchar(256)"},
            {'name': "phone", 'type': "varchar(256)"},
            {'name': "email", 'type': "varchar(256)"}
        ]
    },
    {
        'id': 'event',
        'name': 'Events',
        'fields': [
            {'name': "code", 'type': "varchar(256)"},
            {'name': "name", 'type': "varchar(256)"},
            {'name': "facility_code", 'type': "varchar(256)"},
            {'name': "facility_name", 'type': "varchar(256)"},
            {'name': "group_code", 'type': "varchar(256)"},
            {'name': "group_name", 'type': "varchar(256)"},
            {'name': "type_code", 'type': "varchar(256)"},
            {'name': "type", 'type': "varchar(256)"},
            {'name': "date", 'type': "timestamp"},
            {'name': "keywords", 'type': "varchar"},
            {'name': "tags", 'type': "varchar"}
        ]
    }
]

default_args = {
    'start_date': datetime(2017, 2, 14, 0, 0),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'caa_imap_to_redshift',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
)


with dag:
    kick_off_dag = DummyOperator(task_id='kick_off_dag')

    for workflow in email_workflows:

        type = workflow.get('id', None)
        name = workflow.get('name', None)
        fields = workflow.get('fields', None)

        S3_KEY = '{type}_{time}.csv'.format(type=workflow['id'],
                                            time=TIME)

        imap_to_s3 = ImapToS3Operator(
            task_id='{}_to_s3'.format(type),
            imap_conn_id=IMAP_CONN_ID,
            imap_email=IMAP_EMAIL,
            imap_subject=name,
            s3_conn_id=S3_CONN_ID,
            s3_bucket=S3_BUCKET,
            s3_key=S3_KEY,
        )

        s3_to_redshift = S3ToRedshiftOperator(
            task_id='{}_to_redshift'.format(type),
            s3_conn_id=S3_CONN_ID,
            s3_bucket=S3_BUCKET,
            s3_key=S3_KEY,
            redshift_conn_id=REDSHIFT_CONN_ID,
            redshift_schema=REDSHIFT_SCHEMA,
            table=type,
            origin_schema=fields,
            schema_location='local',
        )

    kick_off_dag >> imap_to_s3 >> s3_to_redshift
