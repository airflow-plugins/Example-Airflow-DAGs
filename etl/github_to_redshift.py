"""
Github to Redshift

This file contains one ongoing hourly DAG.

This DAG makes use of two custom operators:
    - GithubToS3Operator
    https://github.com/airflow-plugins/github_plugin/blob/master/operators/github_to_s3_operator.py#L9
    - S3ToRedshiftOperator
    https://github.com/airflow-plugins/redshift_plugin/blob/master/operators/s3_to_redshift_operator.py#L13

This DAG accesses the following objects:
    - commits
    - issue_comments
    - issues
    - repositories
    - members
    - pull_requests
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import GithubToS3Operator, S3ToRedshiftOperator


S3_CONN_ID = ''
S3_BUCKET = ''
REDSHIFT_CONN_ID = ''
REDSHIFT_SCHEMA = ''
ORIGIN_SCHEMA = ''
SCHEMA_LOCATION = ''
LOAD_TYPE = ''

# For each org being accessed, add additional objects
# with 'name' and 'github_conn_id' to this list.
orgs = [{'name': '',
         'github_conn_id': ''}]

default_args = {'owner': 'airflow',
                'start_date': datetime(2018, 2, 13),
                'email': [''],
                'email_on_failure': True,
                'email_on_retry': False
                }

dag = DAG('github_to_redshift',
          default_args=default_args,
          schedule_interval='@hourly',
          catchup=False
          )


endpoints = [{"name": "commits",
              "payload": {},
              "load_type": "rebuild"},
             {"name": "issue_comments",
              "payload": {"state": "all"},
              "load_type": "rebuild"},
             {"name": "issues",
              "payload": {"state": "all"},
              "load_type": "rebuild"},
             {"name": "repositories",
              "payload": {},
              "load_type": "rebuild"},
             {"name": "members",
              "payload": {},
              "load_type": "rebuild"},
             {"name": "pull_requests",
              "payload": {"state": "all"},
              "load_type": "rebuild"}]


COPY_PARAMS = ["COMPUPDATE OFF",
               "STATUPDATE OFF",
               "JSON 'auto'",
               "TIMEFORMAT 'auto'",
               "TRUNCATECOLUMNS",
               "region as 'us-east-1'"]

with dag:
    kick_off_dag = DummyOperator(task_id='kick_off_dag')

    for endpoint in endpoints:
        for org in orgs:

            S3_KEY = 'github/{0}/{1}.json'.format(org['name'], endpoint['name'])
            TI_PREFIX = 'github_{0}_from_{1}'.format(endpoint['name'], org['name'])
            GITHUB_TASK_ID = '{0}_to_s3'.format(TI_PREFIX)
            REDSHIFT_TASK_ID = '{0}_to_redshift'.format(TI_PREFIX)

            github = GithubToS3Operator(task_id=GITHUB_TASK_ID,
                                        github_conn_id=org['github_conn_id'],
                                        github_org=org['name'],
                                        github_repo='all',
                                        github_object=endpoint['name'],
                                        payload=endpoint['payload'],
                                        s3_conn_id=S3_CONN_ID,
                                        s3_bucket=S3_BUCKET,
                                        s3_key=S3_KEY)

            redshift = S3ToRedshiftOperator(task_id=REDSHIFT_TASK_ID,
                                            s3_conn_id=S3_CONN_ID,
                                            s3_bucket=S3_BUCKET,
                                            s3_key=S3_KEY,
                                            origin_schema=ORIGIN_SCHEMA,
                                            SCHEMA_LOCATION=SCHEMA_LOCATION,
                                            load_type=LOAD_TYPE,
                                            copy_params=COPY_PARAMS,
                                            redshift_schema=REDSHIFT_SCHEMA,
                                            table='{0}_{1}'.format(org['name'],
                                                                   endpoint['name']),
                                            redshift_conn_id=REDSHIFT_CONN_ID,
                                            primary_key='id')

            kick_off_dag >> github >> redshift
