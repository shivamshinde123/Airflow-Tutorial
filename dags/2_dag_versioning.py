
from airflow.sdk import dag, task


@dag
def versioned_dag():

    @task.python
    def first_task():
        print(f"This is the first task")

    @task.python
    def second_task():
        print(f"This is the second task")

    @task.python
    def third_task():
        print(f"This is the third task. DAG Complete!")

    @task.python
    def fourth_task():
        print(f"This is the fourth task")

    # defining the task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    fourth = fourth_task()

    first >> second >> third >> fourth

# instantiating the dag
versioned_dag()
