from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup  # new method to group Tasks - easier

from datetime import datetime


# define a function the return a DAG object - this will be the subdag
def transform_tasks():  # no arguments

    with TaskGroup("transforms", tooltip="Transform tasks") as group:

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

        return group