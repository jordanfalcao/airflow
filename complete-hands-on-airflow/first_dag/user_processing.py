from airflow import DAG
from datetime import datetime

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json # to get data from the api
from pandas import json_normalize # normalize data with PythonOperator


# fuction to be executed by the PythonOperator
def _process_user(ti): # ti: Task Instance, need this parameter to pull the data downloaded by the Task 'extract_user'
    user = ti.xcom_pull(task_ids="extract_user")
    user = user['results'][0]  # json values get into the 'results'
    processed_user = json_normalize({  # 
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email']})
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)

# # function to the fifth Task, storage users using PostgresHook
# def _store_user():
#     hook = PostgresHook(postgres_conn_id = 'postgres')  # using connection created earlier in the first Task
#     hook.copy_expert(  # method to copy from the csv file into the 'users' Table
#         sql="COPY users FROM stdin WITH DELIMITER as ','",  
#         filename='/tmp/processed_user.csv'  # from this file (created on the 2nd Task using _process_user function)
#     )

def _store_user():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )


with DAG (dag_id = "user_processing", start_date = datetime(2023, 1,  1),
          schedule_interval = '@daily', #cron expression
          catchup = False) as dag:
    
    # first Taks: Action type
    create_table = PostgresOperator(
        task_id = 'create_table', # usually set as the same name of the variable
        postgres_conn_id = 'postgres',  # set the connection in the airflow UI, cause is a external connection
        sql = '''
            CREATE TABLE IF NOT EXISTS users(
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        ''')

    # second Task: Sensor type
    is_api_available = HttpSensor(
        task_id='is_api_available',  # usually set as the same name of the variable
        http_conn_id='user_api',  # set the connection in the airflow UI, cause is a external connection
        endpoint='api/'
    )

    # third Task: get data user from the api
    extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter= lambda response: json.loads(response.text),
        log_response = True
    )

    # fourth Task: process the data (normalize)    
    process_user = PythonOperator(
        task_id='process_user',
        python_callable=_process_user # call the python function defined before
    )

    # fifth Task: storage the users
    store_user = PythonOperator(
        task_id='store_user',
        python_callable=_store_user)  # call the python function defined before
    
    # write the dependencies of the Tasks AT THE END OF THE FILE
    create_table >> is_api_available >> extract_user >> process_user >> store_user

    