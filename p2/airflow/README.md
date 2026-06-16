# Apache Airflow 
Plataforma para automatizar, programar y monitorear workflows.

## Caracterisiticas 
Python Puro 
UI moderna 
Facil uso 
Workflow as code 


## Conectores importantes 
[Apache Spark](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html)
[MongoDB](https://airflow.apache.org/docs/apache-airflow-providers-mongo/stable/index.html)

## Terminos importantes

### Dags 

Encapsula lo necesario para ejecutar el workflow
Atributos Dag
* Schedule 
* Tasks: unidades discretas de trabajo que corren en trabajadores 
* Task Dependencies: El orden y condiciones en el que las `tasks` se ejecutan
* Callbacks: Las acciones que se toman al completarse un workflow 

Snippet de codigo:
```py
from datetime import datetime

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

# A Dag represents a workflow, a collection of tasks
with DAG(dag_id="demo", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    @task()
    def airflow():
        print("airflow")

    # Set dependencies between tasks
    hello >> airflow()
```

Explicacion:
Dag llamado `"demo"`, programado para esa fecha. El Dag es como Airflow representa un workflow.
Hay dos `tasks`: Una con el `BashOperator` y otra con `@task` para definir la funcion de python 
El `>>` define una dependencia entre las `tasks` 



## Instalacion en Docker 


### Explicacion del docker file 
Instala los dos providers, pero le pasa a pip un "techo" de versiones.
El constraint file es un txt que Airflow publica oficialmente con todas las versiones exactas de cada dependencia transitiva ya resueltas y probadas para esa combinación de Airflow + Python. Pip lo lee y en vez de explorar el universo de versiones posibles va directo a las que ya sabe que funcionan juntas.
Sin él, pip tiene que resolver solo el árbol de grpcio + pyspark-client + googleapis-common-protos + 40 paquetes más, entra en backtracking y o se cuelga o explota a los 200k rounds como me pasó.