"""
Rate Limit Reseet

This files contains one dag that executes every 12 hours by default.

This DAG should be used in tandem with the RateLimitOperator:
https://github.com/airflow-plugins/rate_limit_plugin/blob/master/operators/rate_limit_operator.py

Every run, this DAG will look for variables with the specified key as it's name.
By default, this key is set to '__SYSTEM__RATE_LIMIT_EXCEEDED__'.

Once found, this DAG will clear the task that caused the variable to be set
(see above Operator) as well as downstream tasks. It will then set the relevant
DAG to "running" and clear the variable.
"""

from datetime import datetime, timedelta
from sqlalchemy import and_
import json

from airflow import DAG
from airflow.models import Variable, TaskInstance, DagRun
from airflow.utils.db import provide_session

from airflow.operators.python_operator import PythonOperator


default_args = {'start_date': datetime(2018, 2, 9),
                'retries': 2,
                'retry_delay': timedelta(minutes=2),
                'email': [],
                'email_on_failure': True}


dag = DAG('__CHECK_FOR_RATE_LIMIT_VARIABLES',
          default_args=default_args,
          schedule_interval='30 */12 * * *',
          catchup=False
          )


@provide_session
def check_py(session=None, **kwargs):
    key = '__SYSTEM__RATE_LIMIT_EXCEEDED__'

    obj = (session
           .query(Variable)
           .filter(Variable.key.ilike('{}%'.format(key)))
           .all())

    if obj is None:
        raise KeyError('Variable {} does not exist'.format(key))
    else:
        for _ in obj:
            _ = json.loads(_.val)

            # Clear the rate limit operator task in the specified Dag Run.
            (session
             .query(TaskInstance)
             .filter(and_(TaskInstance.task_id == _['task_id'],
                          TaskInstance.dag_id == _['dag_id'],
                          TaskInstance.execution_date == datetime.strptime(_['ts'],
                                                                           "%Y-%m-%dT%H:%M:%S")))
             .delete())

            # Clear downstream tasks in the specified Dag Run.
            for task in _['downstream_tasks']:
                (session
                 .query(TaskInstance)
                 .filter(and_(TaskInstance.task_id == task,
                              TaskInstance.dag_id == _['dag_id'],
                              TaskInstance.execution_date == datetime.strptime(_['ts'],
                                                                                "%Y-%m-%dT%H:%M:%S")))
                 .delete())

            # Set the Dag Run state to "running"
            dag_run = (session
                       .query(DagRun)
                       .filter(and_(DagRun.dag_id == _['dag_id'],
                                    DagRun.execution_date == datetime.strptime(_['ts'],
                                                                               "%Y-%m-%dT%H:%M:%S")))
                       .first())

            dag_run.set_state('running')

            # Clear the rate limit exceeded variable.
            variable_identifier = '_'.join([_['dag_id'],
                                            _['task_id'],
                                            _['ts']])

            variable_name = ''.join([key, variable_identifier])

            (session
             .query(Variable)
             .filter(Variable.key == variable_name)
             .delete())


with dag:

    run_check = PythonOperator(task_id='run_check',
                               python_callable=check_py,
                               provide_context=True)

    run_check
