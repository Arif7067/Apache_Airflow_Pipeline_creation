from airflow.sdk import dag, task

@dag(
    dag_id="parallel_tasks"
)

def parallel_tasks():

    @task.python
    def extract_data(**kwargs):
        ti = kwargs['ti']
        print("Extracting the data .......")
        extracted_data_dict = {'api_extracted_data': [1,2,3],
                               'db_extracted_data': [4,5,6],
                               's3_extracted_data': [7,8,9]}
        ti.xcom_push(key='return_result',value=extracted_data_dict)

    @task.python
    def transform_task_api(**kwargs):
        ti =kwargs['ti']
        print("Transforming the api data .......")
        api_data = ti.xcom_pull(task_ids='extract_data',key='return_result')['api_extracted_data']
        api_data = api_data*2
        api_new = {'api_transform_data':api_data}
        ti.xcom_push(key='return_result', value=api_new)

    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs['ti']
        print("tranforming the db task ........")
        db_data = ti.xcom_pull(task_ids='extract_data',key='return_result')['db_extracted_data']
        db_data = list(map(lambda x: x*10, db_data))
        db_new = {'db_transform_data': db_data}
        ti.xcom_push(key='return_result',value=db_new)

    @task.python
    def transform_task_s3(**kwargs):
        ti = kwargs['ti']
        print("Transforming the s3 data ........")
        s3_data = ti.xcom_pull(task_ids='extract_data',key='return_result')['s3_extracted_data']
        s3_data = list(map(lambda x: x+10, s3_data))
        s3_new = {'s3_transform_data':s3_data}
        ti.xcom_push(key='return_result',value=s3_new)

    @task.python
    def load_data(**kwargs):
        ti = kwargs['ti']
        get_api_data = ti.xcom_pull(task_ids='transform_task_api',key='return_result')
        get_db_data = ti.xcom_pull(task_ids='transform_task_db',key='return_result')
        get_s3_data = ti.xcom_pull(task_ids='transform_task_s3',key='return_result')
        load_new = {'transform_data_api':get_api_data,
                    'transform_data_db':get_db_data,
                    'transform_data_s3':get_s3_data}
        print('Final Aggregate Data after tranform ')
        ti.xcom_push(key='return_result',value=load_new)

    first = extract_data()
    second = transform_task_api()
    third = transform_task_db()
    fourth = transform_task_s3()
    fifth = load_data()

    first >> [second, third, fourth] >> fifth

parallel_tasks()

