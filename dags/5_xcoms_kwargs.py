
from airflow.sdk import dag, task


@dag
def xcoms_manual_dag():

    @task.python
    def first_task(**kwargs):

        # extracting task instance from the kwargs to push xcoms manually
        ti = kwargs['ti']

        print(f"Extracting the data... This is the first task")
        fetched_data = {"data": [1, 2, 3, 4, 5]}

        ti.xcom_push(key='fetched_data', value=fetched_data)

    @task.python
    def second_task(**kwargs):

        # extracting the task instance from the kwargs to pull xcoms manually
        ti = kwargs['ti']

        print(f"Tranforming the data... This is the second task")
        fetched_data = ti.xcom_pull(task_ids='first_task', key='fetched_data')['data']

        tranformed_data = fetched_data * 2

        trasformed_data_dict = {"data": tranformed_data}

        ti.xcom_push(key='tranformed_data', value=trasformed_data_dict)

    @task.python
    def third_task(**kwargs):

        # extracting the task instance from the kwargs to pull xcoms manually
        ti = kwargs['ti']
        print(f"This is the third task")
        load_data = ti.xcom_pull(task_ids='second_task', key='tranformed_data_dict')

        return load_data

    # defining the task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third # no need to mention this in airflow 3.0 but doing it just for the sake of doing it.

# instantiating the dag
xcoms_manual_dag()
