from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="gdelt_spark_analytics",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["gdelt", "spark"],
) as dag:

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