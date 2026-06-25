from dag_orchestrate1 import first_orchestrator_dag
from dag_orchestrate2 import second_orchestrator_dag
from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


@dag
def dag_orchestrate_parent():
    
    trigger_first_dag = TriggerDagRunOperator(
        task_id="trigger_first_dag",
        trigger_dag_id="first_orchestrator_dag",
        wait_for_completion=True
    )

    trigger_second_dag = TriggerDagRunOperator(
        task_id="trigger_second_dag",
        trigger_dag_id="second_orchestrator_dag",
        wait_for_completion=True
    )

    trigger_first_dag >> trigger_second_dag


dag_orchestrate_parent()