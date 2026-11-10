from airflow.sdk import dag, task

@dag(
    dag_id="xcom_dag_kwargs"
)

def xcom_dag_kwargs():
    @task.python
    def first_task(**kwargs):
        ti = kwargs['ti']
        print("This is the first task.")
        fetch_data = {"data": [1,2,3,4,5]}
        ti.xcom_push(key='return_result', value=fetch_data)

    @task.python
    def second_task(**kwargs):
        ti = kwargs['ti']
        transform = ti.xcom_pull(task_ids='first_task', key='return_result')['data']
        transform = transform*2
        transfer = {'transf_data': transform}
        ti.xcom_push(key='return_result', value=transfer)
        

    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        loaded_data = ti.xcom_pull(task_ids='second_task', key='return_result')
        return loaded_data

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

#Instantiate the DAG
xcom_dag_kwargs()