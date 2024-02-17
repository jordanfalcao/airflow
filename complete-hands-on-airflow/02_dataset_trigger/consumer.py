from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

# use the same URI from 'producer' DAG
my_file = Dataset("/tmp/my_file.txt")
my_file_2 = Dataset("/tmp/my_file_2.txt")  # another dataset and another URI

# consumer DAG will be triggered when 'my_file' dataset is updated
with DAG(
    dag_id="consumer",
    schedule=[my_file, my_file_2],  # it means everytime the datasets 'my_file' and 'my_file_2' is update,
    start_date=datetime(2023,1,1),           # it triggers this 'consumer' DAG
    catchup=False
):

    # read the dataset and then print
    @task
    def read_dataset():
        with open(my_file.uri, "r") as f:
            print(f.read())

    # read the dataset_2 and then print
    @task
    def read_dataset_2():
        with open(my_file_2.uri, "r") as f:
            print(f.read())

    # task dependency
    read_dataset() >> read_dataset_2()