from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from spotify_etl import run_spotify_etl

with DAG(dag_id = "spotify_analysis", 
    start_date = datetime(2022,1,1), 
    schedule_interval = None, 
    catchup = False) as dag:

    task_a = DummyOperator(
        task_id = "Start",
        dag=dag,
        )

    run_etl = PythonOperator(
    task_id='whole_spotify_etl',
    python_callable=run_spotify_etl,
    dag=dag,
    )

    task_c = DummyOperator(
        task_id = "End",
        dag=dag,
    )

task_a >> run_etl >> task_c
