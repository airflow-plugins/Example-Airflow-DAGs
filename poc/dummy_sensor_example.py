"""
This example dag uses the custom "DummySensorOperator" found here:
https://github.com/airflow-plugins/dummy_sensor_plugin/blob/master/operators/dummy_sensor_operator.py
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# Custom Plugin
from airflow.operators import DummySensorOperator


args = {
    'owner': 'airflow',
    'start_date': datetime(2017, 4, 20, 0, 0),
    'provide_context': True
}

dag = DAG(
    'replicate_skipped_bug',
    schedule_interval="@once",
    default_args=args
)

start = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

dummies = []
with dag:
    for i in range(0, 75):
        d_sensor = DummySensorOperator(
            task_id='dummy_sensor_{}'.format(i),
            timeout=1,
            poke_interval=2,
            flag=False,
            soft_fail=True,
            dag=dag
        )

        d_operator = DummyOperator(task_id='dummy_operator_{}'.format(i))

        start >> d_sensor >> d_operator
