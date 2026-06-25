
from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
        dag_id="schedule_cron_dag",
        start_date=datetime(year=2026, month=1, day=1, tz="America/New_York"),
        schedule=CronTriggerTimetable("0 3 * * 1-5", timezone="America/New_York"),
        end_date=datetime(year=2026, month=12, day=31, tz="America/New_York"),
        catchup=False,
        is_paused_upon_creation=False
)
def schedule_cron_dag():

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
schedule_cron_dag()
