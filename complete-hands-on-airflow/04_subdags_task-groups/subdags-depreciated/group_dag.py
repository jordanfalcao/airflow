from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdag_downloads import subdag_downloads  # importing function from: subdags-folder, subdag_downloads-file, subdag_downloads-function
from subdags.subdag_transforms import subdag_transforms  # importing function from: subdags-folder, subdag_transforms-file, subdag_transforms-function
 
from datetime import datetime
 
 # just Tasks SIMULATION - they do nothing, just wait time
with DAG('group_dag', start_date=datetime(2023, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:

    # the args is the same to use in the parent and sub DAGs
    args = {'start_date': dag.start_date,
             'schedule_interval': dag.schedule_interval,
             'catchup': dag.catchup}
    
    # call SubDagOperator() - change all 3 downloads Tasks for this one
    downloads = SubDagOperator(
        task_id='downloads',
        subdag=subdag_downloads(dag.dag_id, 'downloads', args)  # 'child_dag_id' parameter should be the same as this Task_id: 'downloads'
    )
 
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )
 
    # call SubDagOperator() - change all 3 downloads Tasks for this one
    transforms  = SubDagOperator(
        task_id='transforms',
        subdag=subdag_transforms(dag.dag_id, 'transforms', args)  # 'child_dag_id' parameter should be the same as this Task_id: 'transforms'
    )
 
    # [download_a, download_b, download_c] >> check_files >> [transform_a, transform_b, transform_c]
    downloads >> check_files >> transforms