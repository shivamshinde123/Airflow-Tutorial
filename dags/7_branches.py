
from airflow.sdk import dag, task

@dag
def branch_dag():

    @task.python
    def extract_data_task(**kwargs):
        print(f"Extracting data...")

        ti = kwargs['ti']
        extracted_data_dict = {
            "api_extracted_data": [1,2,3],
            "db_extracted_data": [4,5,6],
            "s3_extracted_data": [7,8,9],
            "weekend_flag": True
        }
        ti.xcom_push(key='extracted_data', value=extracted_data_dict)

    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs['ti']
        extracted_api_data = ti.xcom_pull(task_ids='extract_data_task', key='extracted_data')['api_extracted_data']
        transformed_api_data = [i*10 for i in extracted_api_data]
        ti.xcom_push(key='transformed_api_data', value=transformed_api_data)

    @task.python
    def transform_db_task(**kwargs):
        ti = kwargs['ti']
        extracted_db_data = ti.xcom_pull(task_ids='extract_data_task', key='extracted_data')['db_extracted_data']
        transformed_db_data = [i*100 for i in extracted_db_data]
        ti.xcom_push(key='transformed_db_data', value=transformed_db_data)

    @task.python
    def transform_s3_task(**kwargs):
        ti = kwargs['ti']
        extracted_s3_data = ti.xcom_pull(task_ids='extract_data_task', key='extracted_data')['s3_extracted_data']
        transformed_s3_data = [i*10000 for i in extracted_s3_data]
        ti.xcom_push(key='transformed_s3_data', value=transformed_s3_data)

    # creating the decider node
    @task.branch
    def decider_task(**kwargs):
        ti = kwargs['ti']
        weekend_flag = ti.xcom_pull(task_ids='extract_data_task', key='extracted_data')['weekend_flag']

        if weekend_flag:
            return 'no_load_task' # you should use the function name here
        else:
            return 'load_task' # you should use the function name here


    @task.bash
    def load_task(**kwargs):
        ti = kwargs['ti']
        transformed_api_data = ti.xcom_pull(task_ids='transform_task_api', key='transformed_api_data')
        transformed_db_data = ti.xcom_pull(task_ids='transform_db_task', key='transformed_db_data')
        transformed_s3_data = ti.xcom_pull(task_ids='transform_s3_task', key='transformed_s3_data')

        return f"echo 'Loaded Data: {transformed_api_data}, {transformed_db_data}, {transformed_s3_data}'"
    
    @task.bash
    def no_load_task(**kwargs):
        print(f"No loading on weekends...")
        return "echo 'No load task executed...'"
    

    # defining the task dependencies
    extract_data = extract_data_task()

    transform_api = transform_task_api()
    transform_db = transform_db_task()
    transform_s3 = transform_s3_task()

    load_data = load_task()
    no_load_data = no_load_task()

    decider = decider_task()

    extract_data >> [transform_api, transform_db, transform_s3] >> decider >> [load_data, no_load_data]




# instantiating the dag
branch_dag()
