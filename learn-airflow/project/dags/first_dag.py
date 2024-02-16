try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def first_function_execute(**context):
    print("first_function_execute   ")  # push context['ti'] to another function gets
    context['ti'].xcom_push(key='mykey', value="first_function_execute says Hello")


def second_function_execute(**context):
    instance = context.get("ti").xcom_pull(key="mykey")  # pull context['ti'] from the another function
    data = [{"name": "Jordan", "title": "Future Data Engineer"},
            {"name": "Jams", "title": "Future Public Servant"}, ]

    df = pd.DataFrame(data=data) # import and use Pandas library
    print('@' * 66)
    print(df.head())   # print 10 first names (we only have two)
    print('@' * 66)
    print("I am in second_function_execute got value: {} from Function 1 ".format(instance))


# */2 * * * * Execute every Two Minutes (example)
with DAG(
        dag_id="first_dag",  # we've set the name
        schedule_interval="@daily",  # default, but can be changed
        default_args={
            "owner": "airflow",
            "retries": 1,  # retry once if it gets error
            "retry_delay": timedelta(minutes=5),   # retry after 5 minutes
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:
    first_function_execute = PythonOperator(
        task_id="first_function_execute",  # just a name
        python_callable=first_function_execute,  # just a name
        provide_context=True,        # turn into True
        # op_kwargs={"name": "Jordan Falcao"} # used in the previous example to get **kwargs "name" and print it
    )

    second_function_execute = PythonOperator(
        task_id="second_function_execute",  # just a name
        python_callable=second_function_execute,  # just a name
        provide_context=True,         # turn into True too
        # op_kwargs={"name": "Jordan Falcao"}
    )

# arrow (one function depends on the other)
first_function_execute >> second_function_execute
