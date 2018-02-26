"""
MongoDB to Redshift

This file contains one ongoing daily DAG.

This DAG makes use of two custom operators:
    - MongoToS3Operator
    https://github.com/airflow-plugins/mongo_plugin/blob/master/operators/mongo_to_s3_operator.py#L9
    - S3ToRedshiftOperator
    https://github.com/airflow-plugins/redshift_plugin/blob/master/operators/s3_to_redshift.py#L13

This DAG also uses a Mongo collection processing script that accepts a
json formatted Mongo schema mapping and outputs both a Mongo query projection
and a compatible Redshift schema mapping. This script can be found in
"etl/mongo_to_redshift/collections/_collection_processing.py".

This DAG also contains a flattening script that removes invalid characters
from the Mongo keys as well as scrubbing out the "_$date" suffix that
PyMongo appends to datetime fields.
"""
from datetime import datetime, timedelta
import json
import re
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks import S3Hook
from airflow.operators import (MongoToS3Operator,
                               S3ToRedshiftOperator,
                               PythonOperator)

from mongo_to_redshift.collections._collection_processing import _prepareData

S3_CONN = ''
S3_BUCKET = ''
REDSHIFT_SCHEMA = ''
REDSHIFT_CONN_ID = ''
MONGO_CONN_ID = ''
MONGO_DATABASE = ''

default_args = {'start_date': datetime(2018, 2, 22),
                'retries': 2,
                'retry_delay': timedelta(minutes=5),
                'email': [],
                'email_on_failure': True,
                'email_on_retry': False}

dag = DAG('mongo_to_redshift_daily',
          default_args=default_args,
          schedule_interval='@daily',
          catchup=True)

cwd = os.getcwd()


def process_collection(collection):
    file_path = 'dags/etl/mongo_to_redshift/collections/{0}.json'.format(collection)
    new_path = os.path.join(cwd, file_path)
    order = json.load(open(new_path))
    projection, schema = _prepareData(order)


collections = [{'name': 'example_mongo_collection',
                'collection': 'example_mongo_collection',
                'mongo_query': [{"$match": {"dateLastActivity": {
                                 "$lt": '{{ next_execution_date }}',
                                 "$gte": '{{ execution_date }}'
                                 }}},
                                {"$project": process_collection('example_mongo_collection')[0]}],
                'schema': process_collection('example_mongo_collection')[1],
                'primary_key': ["id"],
                'incremental_key': 'dateLastActivity',
                'load_type': 'upsert'}]


def flatten_py(**kwargs):
    s3_key = kwargs['templates_dict']['s3_key']
    flattened_key = kwargs['templates_dict']['flattened_key']
    s3_conn = kwargs['templates_dict']['s3_conn']
    s3_bucket = kwargs['templates_dict']['s3_bucket']
    s3 = S3Hook(s3_conn)

    output = (s3.get_key(s3_key,
                         bucket_name=s3_bucket)
                .get_contents_as_string(encoding='utf-8'))

    output = output.split('\n')

    output = '\n'.join([json.dumps({re.sub(r'[?|$|.|!]', r'', k.lower().replace('_$date', '')): v for k, v in i.items()}) for i in output])

    s3.load_string(output,
                   flattened_key,
                   bucket_name=s3_bucket,
                   replace=True)


with dag:
    kick_off_dag = DummyOperator(task_id='kick_off_dag')

    for collection in collections:
        if 'collection' in collection.keys():
            collection_name = collection['collection']
        else:
            collection_name = collection['name']

        if 'incremental_key' in collection.keys():
            incremental_key = collection['incremental_key']
        else:
            incremental_key = None

        S3_KEY = 'mongo/raw/{0}_{1}.json'.format(collection['name'], '{{ ts_nodash}}')

        FLATTENED_KEY = 'mongo/flattened/{0}_{1}_flattened.json'.format(collection['name'], '{{ ts_nodash}}')

        mongo = MongoToS3Operator(task_id='{0}_to_s3'.format(collection['name']),
                                  mongo_conn_id=MONGO_CONN_ID,
                                  mongo_collection=collection_name,
                                  mongo_database=MONGO_DATABASE,
                                  mongo_query=collection['mongo_query'],
                                  s3_conn_id=S3_CONN,
                                  s3_bucket=S3_BUCKET,
                                  s3_key=S3_KEY,
                                  replace=True
                                  )

        flatten_object = PythonOperator(task_id='flatten_{0}'.format(collection['name']),
                                        python_callable=flatten_py,
                                        templates_dict={'s3_key': S3_KEY,
                                                        's3_conn': S3_CONN,
                                                        's3_bucket': S3_BUCKET,
                                                        'collection_name': collection['name'],
                                                        'flattened_key': FLATTENED_KEY,
                                                        'origin_schema': collection['schema']},
                                        provide_context=True)

        redshift = S3ToRedshiftOperator(task_id='{0}_to_redshift'.format(collection['name']),
                                        s3_conn_id=S3_CONN,
                                        s3_bucket=S3_BUCKET,
                                        s3_key=FLATTENED_KEY,
                                        redshift_conn_id=REDSHIFT_CONN_ID,
                                        redshift_schema=REDSHIFT_SCHEMA,
                                        origin_schema=collection['schema'],
                                        redshift_table=collection['name'],
                                        primary_key=collection.get('primary_key', None),
                                        incremental_key=incremental_key,
                                        load_type=collection['load_type'])

        kick_off_dag >> mongo >> flatten_object >> redshift
