from airflow.sdk import dag, task

@dag(
    dag_id="xcom_dag_auto"
)

def xcom_dag_auto():
    @task.python
    def first_task():
        print("This is the first task.")
        fetched_data = {"data": [1,2,3,4,5]}
        return fetched_data

    @task.python
    def second_task(data):
        transform = data["data"]
        transform = 2 * transform
        transformed_data = {'transf_data': transform}
        return transformed_data

    @task.python
    def third_task(data):
        loaded_data = data
        return loaded_data

    first = first_task()
    second = second_task(first)
    third = third_task(second)

#Instantiate the DAG
xcom_dag_auto()