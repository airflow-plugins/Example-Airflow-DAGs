# Originally from @adamhaney -- https://gist.github.com/adamhaney/916a21b0adcabef4038c38e3ac96a36f

from datetime import datetime, timedelta
import networkx as nx

from airflow import DAG
from airflow.operators import BashOperator, SubDagOperator

start_date = datetime(year=2017, month=6, day=13, hour=19, minute=0)
schedule_interval = '0 * * * 1-5'

default_args = {
    'owner': 'The Owner',
    'email': ['theoperator@email.com'],
    'retry_interval': timedelta(minutes=15),
    'sla': timedelta(minutes=60),
    'depends_on_downstream': True,
    'email_on_failure': True,
    'email_on_retry': True,
    'provide_context': True,
}


dag = DAG('dbt', start_date=start_date, schedule_interval=schedule_interval, default_args=default_args, max_active_runs=1)

dbt_clone = BashOperator(
    task_id='dbt_clone',
    bash_command='cd ~/project && git fetch --all && git reset --hard origin/master',
    dag=dag
)

dbt_deps = BashOperator(
    task_id='dbt_deps',
    bash_command='cd ~/project && dbt deps  --profile=warehouse --target=prod',
    dag=dag
)

dbt_deps.set_upstream(dbt_clone)

dbt_clean = BashOperator(
    task_id='dbt_clean',
    bash_command='cd ~/project && dbt clean  --profile=warehouse --target=prod',
    dag=dag
)
dbt_clean.set_upstream(dbt_deps)

dbt_archive = BashOperator(
    task_id='dbt_archive',
    bash_command='cd ~/project && dbt archive  --profile=warehouse --target=prod',
    dag=dag
)

dbt_archive.set_upstream(dbt_clean)

dbt_seed = BashOperator(
    task_id='dbt_seed',
    bash_command='cd ~/gospel && dbt seed  --profile=warehouse --target=prod',
    dag=dag
)

dbt_seed.set_upstream(dbt_archive)

dbt_compile = BashOperator(
    task_id='dbt_compile',
    bash_command='''cd ~/project && dbt compile  --profile=warehouse --target=prod && find target/build | xargs -I {} bash -c "echo '-------------------- '{}' --------------------' && cat {} || exit 0"''',
    dag=dag
)

dbt_compile.set_upstream(dbt_seed)

copy_gpickle_file = BashOperator(
    task_id='copy_gpickle_file',
    bash_command='''cp ~/project/target/graph.gpickle ~/project/graph.gpickle''',
    dag=dag
)

copy_gpickle_file.set_upstream(dbt_compile)

def dbt_dag(start_date, schedule_interval, default_args):
    temp_dag = DAG('gospel_.dbt_sub_dag', start_date=start_date, schedule_interval=schedule_interval, default_args=default_args)
    G = nx.read_gpickle('/home/airflowuser/project/graph.gpickle')

    def make_dbt_task(model_name):
        simple_model_name = model_name.split('.')[-1]
        dbt_task = BashOperator(
                    task_id=model_name,
                    bash_command='cd ~/gospel && dbt run  --profile=warehouse --target=prod --non-destructive --models {simple_model_name}'.format(simple_model_name=simple_model_name),
                    dag=temp_dag
                    )
        return dbt_task


    dbt_tasks = {}
    for node_name in set(G.nodes()):
        dbt_task = make_dbt_task(node_name)
        dbt_tasks[node_name] = dbt_task

    for edge in G.edges():
        dbt_tasks[edge[0]].set_downstream(dbt_tasks[edge[1]])
    return temp_dag

dbt_sub_dag = SubDagOperator(
    subdag=dbt_dag(dag.start_date, dag.schedule_interval, default_args=default_args),
    task_id='dbt_sub_dag',
    dag=dag,
    trigger_rule='all_done'
)
dbt_sub_dag.set_upstream(copy_gpickle_file)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd ~/project && dbt test  --profile=warehouse --target=prod',
    dag=dag
)
dbt_test.set_upstream(dbt_sub_dag)
