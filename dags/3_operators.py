
from airflow.sdk import dag, task


@dag
def operators_dag():

    @task.python
    def first_task():
        print(f"This is the first task")

    @task.python
    def second_task():
        print(f"This is the second task")

    @task.bash
    def run_after_loop() -> str:
        return "echo Shivam Shinde"

    # defining the task dependencies
    first = first_task()
    second = second_task()
    third = run_after_loop()

    first >> second >> third

# instantiating the dag
operators_dag()
