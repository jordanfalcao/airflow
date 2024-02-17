from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

# create datasets using the URI to interact with the DAG
my_file = Dataset("/tmp/my_file.txt")  # this path is the URI
my_file_2 = Dataset("/tmp/my_file_2.txt")  # another dataset and another URI

with DAG(
    dag_id="producer",
    start_date=datetime(2023,1,1),
    schedule='@daily',
    catchup=False
) as dag:

    # first task using the @task decorator
    # as soon as this task succeeds, the DAG that depends on this dataset is automatically triggered
    @task(outlets=[my_file])  # 'outlets' parameter indicates to airflow that this task updates the 'my_file' dataset
    def update_dataset():
        with open(my_file.uri, "a+") as f:  # a+ to append or create if not exist
            f.write("producer update")

    # second Task to update another dataset
    @task(outlets=[my_file_2])  # 'outlets' parameter indicates to airflow that this task updates the 'my_file' dataset
    def update_dataset_2():
        with open(my_file_2.uri, "a+") as f:  # a+ to append or create if not exist
            f.write("producer update")

    # the second Task depends on the first one
    update_dataset() >> update_dataset_2()