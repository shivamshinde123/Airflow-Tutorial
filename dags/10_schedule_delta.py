
from airflow.sdk import dag, task
from pendulum import datetime, duration # better than python's built in datetime module
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
        dag_id="schedule_delta_dag",
        start_date=datetime(year=2026, month=1, day=1, tz="America/New_York"),
        schedule=DeltaTriggerTimetable(duration(days=3)),
        end_date=datetime(year=2026, month=12, day=31, tz="America/New_York"),
        catchup=False,
        is_paused_upon_creation=False
)
def schedule_delta_dag():

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
schedule_delta_dag()
