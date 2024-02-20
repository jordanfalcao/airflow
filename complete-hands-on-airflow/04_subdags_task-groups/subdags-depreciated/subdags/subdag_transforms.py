from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime


# define a function the return a DAG object - this will be the subdag
def subdag_transforms(parent_dag_id, child_dag_id, args):  # args must be defined in the parent DAG

    with DAG(f"{parent_dag_id}.{child_dag_id}",  # group_dag.downloads - the name of the example
             start_date=args['start_date'],
             schedule_interval=args['schedule_interval'],  # [''] get the args values from the parent DAG
             catchup=args['catchup']) as dag:

        # code the 3 Tasks we want to group
        transform_a = BashOperator(
            task_id='transform_a',
            bash_command='sleep 10'
        )
    
        transform_b = BashOperator(
            task_id='transform_b',
            bash_command='sleep 10'
        )
    
        transform_c = BashOperator(
            task_id='transform_c',
            bash_command='sleep 10'
        )

        return dag