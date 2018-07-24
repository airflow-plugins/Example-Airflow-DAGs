"""
An example DAG using the IMAP plugin to:
1) Access an IMAP Server.
2) Search the inbox for an email with a specific subject
3) Extract the body of the specified email
4) Parse the body according to a Regular Expression
5) Return the parsed result as an XCOM

This files contains one dag:
    - An ongoing daily workflow.

Each DAG makes use of one custom hook:
    - ImapHook
    https://github.com/airflow-plugins/imap_plugin/blob/master/hooks/imap_hook.py#L9

"""

from datetime import datetime, timedelta
import re
import logging

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from imap_plugin.hooks.imap_hook import ImapHook

IMAP_CONN_ID = ''
SUBJECT = ''
SEARCH_STRING = r''

default_args = {
    'start_date': datetime(2018, 2, 10, 0, 0),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'imap_inbox_search',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
)


def imap_py(**kwargs):
    imap_conn_id = kwargs.get('templates_dict', None).get('imap_conn_id', None)
    imap_subject = kwargs.get('templates_dict', None).get('imap_subject', None)
    reg_ex = kwargs.get('templates_dict', None).get('reg_ex', None)

    search_criteria = '(HEADER Subject "{}")'.format(imap_subject)

    logging.info('Retrieving emails...')

    message = ImapHook(imap_conn_id=imap_conn_id).read_body(search_criteria)

    logging.info('Successfully retrieved email...')

    logging.info('Parsing Email...')

    result = re.search(reg_ex, message).group(0)

    logging.info('Result: {}'.format(result))

    return result


with dag:

    kick_off_dag = DummyOperator(task_id='kick_off_dag')

    imap = PythonOperator(
        task_id='imap_search',
        python_callable=imap_py,
        templates_dict={"imap_conn_id": IMAP_CONN_ID,
                        "imap_subject": SUBJECT,
                        "reg_ex": SEARCH_STRING
                        },
        provide_context=True
    )

    kick_off_dag >> imap
