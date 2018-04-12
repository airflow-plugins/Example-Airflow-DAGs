"""
SFTP to Mongo

This files contains an ongoing daily workflow that:
    1) Looks for a csv in SFTP
    2) If that file exists, retrieves it.
    3) Converts that csv to json.
    4) Inserts these records into Mongo.

Throughout this process, S3 is used as a intermediary storage layer.

This DAG makes use of two custom operators:
    - S3ToMongoOperator
    https://github.com/airflow-plugins/mongo_plugin/blob/master/operators/s3_to_mongo_operator.py#L11
    - SFTPToS3Operator
    https://github.com/airflow-plugins/sftp_plugin/blob/master/operators.py/sftp_to_s3_operator.py#L7

"""
from datetime import datetime, timedelta
from flatten_json import unflatten_list
import pandas as pd
import logging
import json

from airflow import DAG
from airflow.hooks import S3Hook, SSHHook
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator

from mongo_plugin.operators.s3_to_mongo_operator import S3ToMongoOperator
from sftp_plugin.operators.sftp_to_s3_operator import SFTPToS3Operator


SSH_CONN_ID = ''

MONGO_CONN_ID = ''
MONGO_DB = ''
MONGO_COLLECTION = ''

FILENAME = ''
FILEPATH = ''
SFTP_PATH = '/{0}/{1}'.format(FILEPATH, FILENAME)

S3_CONN_ID = ''
S3_BUCKET = ''
S3_KEY = ''

today = "{{ ds }}"

S3_KEY_TRANSFORMED = '{0}_{1}.json'.format(S3_KEY, today)

default_args = {
    'start_date': datetime(2018, 4, 12, 0, 0, 0),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'sftp_to_mongo',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=True
)


def check_for_file_py(**kwargs):
    path = kwargs.get('path', None)
    sftp_conn_id = kwargs.get('sftp_conn_id', None)
    filename = kwargs.get('templates_dict').get('filename', None)
    ssh_hook = SSHHook(ssh_conn_id=sftp_conn_id)
    sftp_client = ssh_hook.get_conn().open_sftp()
    ftp_files = sftp_client.listdir(path)

    logging.info('Filename: ' + str(filename))
    if filename in ftp_files:
        return True
    else:
        return False


def transform_py(**kwargs):
    s3 = kwargs.get('s3_conn_id', None)
    s3_key = kwargs.get('templates_dict').get('s3_key', None)
    transformed_key = kwargs.get('templates_dict').get('transformed_key', None)

    s3_bucket = kwargs.get('s3_bucket', None)
    hook = S3Hook(s3)

    (hook.get_key(s3_key,
                  bucket_name=s3_bucket)
         .get_contents_to_filename('temp.csv'))

    df = pd.read_csv('temp.csv')

    records = json.loads(df.to_json(orient='records'))
    del df

    records = [unflatten_list(record) for record in records]

    records = '\n'.join([json.dumps(record) for record in records])

    hook.load_string(string_data=records,
                     key=transformed_key,
                     bucket_name=s3_bucket,
                     replace=True)


with dag:
    files = ShortCircuitOperator(task_id='check_for_file',
                                 python_callable=check_for_file_py,
                                 templates_dict={'filename': FILENAME},
                                 op_kwargs={"path": FILEPATH,
                                            "sftp_conn_id": SSH_CONN_ID},
                                 provide_context=True)

    sftp = SFTPToS3Operator(
            task_id='retrieve_file',
            sftp_conn_id=SSH_CONN_ID,
            sftp_path=SFTP_PATH,
            s3_conn_id=S3_CONN_ID,
            s3_bucket=S3_BUCKET,
            s3_key=S3_KEY
    )

    transform = PythonOperator(task_id='transform_to_json',
                               python_callable=transform_py,
                               templates_dict={'s3_key': S3_KEY,
                                               'transformed_key': S3_KEY_TRANSFORMED},
                               op_kwargs={"s3_conn_id": S3_CONN_ID,
                                          "s3_bucket": S3_BUCKET},
                               provide_context=True)

    mongo = S3ToMongoOperator(
        task_id='sink_to_mongo',
        s3_conn_id=S3_CONN_ID,
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY_TRANSFORMED,
        mongo_conn_id=MONGO_CONN_ID,
        mongo_collection=MONGO_COLLECTION,
        mongo_db=MONGO_COLLECTION,
        mongo_method='replace',
        mongo_replacement_filter='',
        upsert=True
    )

    files >> sftp >> transform, mongo
