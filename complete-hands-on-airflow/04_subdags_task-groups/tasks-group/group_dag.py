from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.operators.subdag import SubDagOperator  # depreciated
from groups.group_downloads import download_tasks  # importing function from: groups-folder, group_downloads-file, download_tasks-function
from groups.group_transforms import transform_tasks  # importing function from: groups-folder, group_transforms-file, transform_tasks-function
 
from datetime import datetime
 
 # just Tasks SIMULATION - they do nothing, just wait time
with DAG('group_dag', start_date=datetime(2023, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
   
    # new method to group DAG, easier than subdags
    downloads = download_tasks()
 
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )
 
    # new method to group DAG, easier than subdags
    transforms = transform_tasks()
 
    # [download_a, download_b, download_c] >> check_files >> [transform_a, transform_b, transform_c]
    downloads >> check_files >> transforms