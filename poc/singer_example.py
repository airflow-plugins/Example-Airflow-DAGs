"""
Singer

This example shows how to use Singer within Airflow using a custom operator:
- SingerOperator
https://github.com/airflow-plugins/singer_plugin/blob/master/operators/singer_operator.py#L5

A complete list of Taps and Targets can be found in the Singer.io Github org:
https://github.com/singer-io
"""


from datetime import datetime

from airflow import DAG

from airflow.operators import SingerOperator

default_args = {'start_date': datetime(2018, 2, 22),
                'retries': 0,
                'email': [],
                'email_on_failure': True,
                'email_on_retry': False}

dag = DAG('__singer__fixerio_to_csv',
          schedule_interval='@hourly',
          default_args=default_args)

with dag:

    singer = SingerOperator(task_id='singer',
                            tap='fixerio',
                            target='csv')

    singer
