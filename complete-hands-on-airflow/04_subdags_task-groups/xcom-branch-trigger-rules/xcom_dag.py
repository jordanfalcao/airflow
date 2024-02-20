from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
 
from datetime import datetime
 
### SHARING DATA BETWEEN TASKS WITH XCOM

# PUSH xcom IN the metadatabase of the airflow
def _t1(ti):  # ti = task instance object
    ti.xcom_push(key='my_key', value=42)  # xcom_push method used to push xcom in the metadatabase
 

# PULL xcom FROM the metadatabase of the airflow
def _t2(ti):  # ti = task instance object
    ti.xcom_pull(key='my_key', task_ids='t1')  # xcom_pull method used to pull xcom from the metadatabase, specify the task_id


def _branch(ti):
    value = ti.xcom_pull(key='my_key', task_ids='t1')  # value var contains xcom pushed by t1
    if value == 42:  # if the value pulled is equal to 42, execute Task t2
        return 't2'
    return 't3'      # else, execute Task t3
 
with DAG("xcom_dag", start_date=datetime(2023, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
 
    # push xcom in the metadatabase
    t1 = PythonOperator(
        task_id='t1',
        python_callable=_t1
    )

    # BRANCH Task - execute a Taks according to a condition
    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=_branch
    )
 
    # pull xcom from the metadatabase
    t2 = PythonOperator(
        task_id='t2',
        python_callable=_t2
    )
 
    t3 = BashOperator(
        task_id='t3',
        bash_command="echo ''"
    )

    t4 = BashOperator(
        task_id='t4',
        bash_command="echo ''",
        trigger_rule='none_failed_min_one_success'  # execute if one is succeed and the others is skipped
    )
 
    # branch Task pulls the value from t1 and then execute t2 or t3 according to the value
    t1 >> branch >> [t2, t3] >> t4