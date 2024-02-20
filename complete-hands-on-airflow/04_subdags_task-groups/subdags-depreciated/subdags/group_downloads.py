from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime


# define a function the return a DAG object - this will be the subdag
def subdag_downloads(parent_dag_id, child_dag_id, args):  # args must be defined in the parent DAG

    with DAG(f"{parent_dag_id}.{child_dag_id}",  # group_dag.downloads - the name of the example
             start_date=args['start_date'],
             schedule_interval=args['schedule_interval'],  # [''] get the args values from the parent DAG
             catchup=args['catchup']) as dag:

        # code the 3 Tasks we want to group
        download_a = BashOperator(
            task_id='download_a',
            bash_command='sleep 10'
        )

        download_b = BashOperator(
            task_id='download_b',
            bash_command='sleep 10'
        )

        download_c = BashOperator(
            task_id='download_c',
            bash_command='sleep 10'
        )

        return dag