from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator



@dag(
    dag_id="operators_dag",
)

def operators_dag():
    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
        print("This is the third task")

    @task.bash
    def bash_task_modern() -> str:
        return "echo https://airflow.apache.org/"

    bash_task_oldschool = BashOperator(
        task_id="bash_task_oldschool",
        bash_command="echo https://airflow.apache.org/",
    )


    # Define the task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    bash_modern = bash_task_modern()
    bash_oldschool = bash_task_oldschool

    first >> second >> third >> bash_modern >> bash_oldschool

#Instantiate the DAG
operators_dag()