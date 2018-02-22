"""
This example uses the existing Dummy Operator and Variable model to
demonstrate dynamic creation of DAGs based on a Variable setting. As
shown below, a list of customer objects is retrieved and used to create
unique dags based on the imput.
"""

from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

# Create JSON Variable if it doesn't exist

CUSTOMERS = [
    {
        'customer_name': 'Faux Customer',
        'customer_id': 'faux_customer',
        'email': ['admin@fauxcustomer.com', 'admin@astronomer.io'],
        'schedule_interval': None,
        'enabled': True
    },
    {
        'customer_name': 'Bogus Customer',
        'customer_id': 'bogus_customer',
        'email': ['admin@boguscustomer.com', 'admin@astronomer.io'],
        'schedule_interval': '@once',
        'enabled': True
    }
]

# Get JSON Variable
CUSTOMERS = Variable.get("customer_list",
                         default_var=CUSTOMERS,
                         deserialize_json=True)


def create_dag(customer):
    """
    Accepts a customer parameters dict and
    overrides default args to create a DAG object

    Returns: DAG() Object
    """
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email': 'xyz@xyz.com',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'start_date': datetime(2017, 1, 1, 0, 0),
        'end_date': None
    }

    """
    This allows DAG parameters to be passed in from the Variable if
    a customer needs something specific overridden in their DAG.
    Consider how email being passed in from the customer object
    overrides email in the resulting replaced_args object.
    """
    replaced_args = {k: default_args[k] if customer.get(
        k, None) is None else customer[k] for k in default_args}

    dag_id = '{base_name}_{id}'.format(
        base_name='load_clickstream_data', id=customer['customer_id'])

    return DAG(dag_id=dag_id,
               default_args=replaced_args,
               schedule_interval=customer['schedule_interval'])


# Loop customers array of containing customer objects
    for cust in CUSTOMERS:
        if cust['enabled']:

            dag = create_dag(cust)

            globals()[dag.dag_id] = dag

            extract = DummyOperator(
                task_id='extract_data',
                dag=dag
            )

            transform = DummyOperator(
                task_id='transform_data',
                dag=dag
            )

            load = DummyOperator(
                task_id='load_data',
                dag=dag
            )

            extract >> transform >> load

        else:
            # TODO Create but programmatically pause
            pass
