"""
Dynamic Connection Creation from a Variable

This file contains one ongoing DAG that executes every 15 minutes.

This DAG makes use of one custom operator:
    - CreateConnectionsFromVariable
    https://github.com/airflow-plugins/variable_connection_plugin/blob/master/operator/variable_connection_operator.py#L36

If using encrypted tokens in the Variable (recommended), it is necessary
to create a separate "Fernet Key Connection" with the relevant Fernet Key
kept in the password field. This Conn ID can then be specified in the
operator below.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators import CreateConnectionsFromVariable

FERNET_KEY_CONN_ID = None
CONFIG_VARIABLE_KEY = ''

args = {
    'owner': 'airflow',
    'start_date': datetime(2018, 2, 22, 0, 0),
    'provide_context': True,
    'email': [],
    'email_on_failure': True
}

dag = DAG(
    '__VARIABLE_CONNECTION_CREATION__',
    schedule_interval="*/15 * * * *",
    default_args=args,
    catchup=False
)

create_airflow_connections = CreateConnectionsFromVariable(
    task_id='create_airflow_connections',
    fernet_key_conn_id=FERNET_KEY_CONN_ID,
    config_variable_key=CONFIG_VARIABLE_KEY,
    dag=dag)

create_airflow_connections
