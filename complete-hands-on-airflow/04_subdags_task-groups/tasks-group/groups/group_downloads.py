from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup  # new method to group Tasks - easier

from datetime import datetime


# define a function the return a GROUP object
def download_tasks():  # without arguments

    with TaskGroup("downloads", tooltip="Download tasks") as group:

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

        return group