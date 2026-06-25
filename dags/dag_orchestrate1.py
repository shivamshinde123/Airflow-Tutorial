
from airflow.sdk import dag, task


@dag
def first_orchestrator_dag():

    @task.python
    def first_task():
        print(f"This is the first task")

    @task.python
    def second_task():
        print(f"This is the second task")

    @task.python
    def third_task():
        print(f"This is the third task")

    # defining the task dependencies
    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third



# instantiating the dag
first_orchestrator_dag()
