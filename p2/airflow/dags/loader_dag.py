from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

from loader.stream_manager import main
from loader.cleanup import borrar_parquets_raw

with DAG(
    dag_id="gdelt_pipeline_15min",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gdelt", "pipeline"],
) as dag:

    descargar = PythonOperator(
        task_id="descargar_gdelt",
        python_callable=main
    )

    ejecutar_analytics = BashOperator(
        task_id="ejecutar_analytics",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --conf spark.jars.ivy=/tmp/ivy \
          --packages org.mongodb.spark:mongo-spark-connector_2.13:10.5.0 \
          /opt/spark-jobs/analytics.py
        """
    )

    limpiar_raw = PythonOperator(
        task_id="limpiar_raw_viejo",
        python_callable=borrar_parquets_raw
    )

    descargar >> ejecutar_analytics >> limpiar_raw
