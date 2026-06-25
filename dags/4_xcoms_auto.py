
from airflow.sdk import dag, task


@dag
def xcoms_auto_dag():

    @task.python
    def first_task():
        print(f"Extracting the data... This is the first task")
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        return fetched_data

    @task.python
    def second_task(data: dict):
        print(f"Tranforming the data... This is the second task")
        fetched_data = data['data'] 
        tranformed_data = fetched_data * 2
        trasformed_data_dict = {"data": tranformed_data}
        return trasformed_data_dict

    @task.python
    def third_task(data: dict):
        print(f"This is the third task")
        load_data = data
        return load_data

    # defining the task dependencies
    first = first_task()
    second = second_task(first)
    third = third_task(second)

    first >> second >> third # no need to mention this in airflow 3.0 but doing it just for the sake of doing it.

# instantiating the dag
xcoms_auto_dag()
