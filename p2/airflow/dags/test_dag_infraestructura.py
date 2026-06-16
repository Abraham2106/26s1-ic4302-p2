from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="test_dag_infraestructura",
    schedule=None,
    start_date=datetime(2026, 6, 15),
    catchup=False,
    tags=["test"],
) as dag:
    t1 = BashOperator(task_id="paso_1", bash_command="echo 'ok'")
    t2 = BashOperator(task_id="paso_2_a", bash_command="sleep 2 && echo 'a'")
    t3 = BashOperator(task_id="paso_2_b", bash_command="sleep 2 && echo 'b'")
    t1 >> [t2, t3]