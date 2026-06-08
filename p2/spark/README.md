# Apache Spark — Guía para el equipo

## ¿Qué es Spark y por qué lo usamos?

Apache Spark es un motor de procesamiento de datos distribuido. "Distribuido" significa que el trabajo se divide entre varias máquinas (o contenedores) que colaboran en paralelo. En este proyecto, Spark es el componente que toma los archivos Parquet que genera el Loader, ejecuta los 19 análisis geopolíticos, y escribe los resultados en MongoDB.

La razón de usar Spark en lugar de un script Python común es el volumen de datos: GDELT publica nuevos archivos cada 15 minutos, 24/7. Un script secuencial tardaría demasiado. Spark parte el problema en pedazos y los procesa en paralelo.

---

## Arquitectura del cluster en este proyecto

```
┌─────────────────────────────────────────────────────┐
│                   Docker network                    │
│                                                     │
│  ┌──────────────┐     ┌────────────┐  ┌──────────┐ │
│  │ spark-master │────▶│  worker-1  │  │ worker-2 │ │
│  │  :8080 UI   │     │            │  │          │ │
│  │  :7077 RPC  │     └────────────┘  └──────────┘ │
│  └──────────────┘                                   │
│         ▲                                           │
│         │ SparkSubmitOperator                       │
│  ┌──────────────┐                                   │
│  │   Airflow    │  (se integra en sprints futuros)  │
│  └──────────────┘                                   │
└─────────────────────────────────────────────────────┘
```

- **Master**: coordina el trabajo, no procesa datos. Expone la Web UI en `localhost:8080` y el puerto RPC `7077` para recibir jobs.
- **Workers**: ejecutan las tareas reales. Cada uno tiene 1 CPU y 1 GB de RAM asignados.
- **Jobs**: scripts Python (PySpark) que se envían al master con `spark-submit`. El master los distribuye a los workers.

---

## El flujo de datos en este pipeline

```
GDELT (web)
    │
    ▼
Loader → JSON → transformer.py → Parquet
                                    │
                                    ▼
                              Spark lee Parquet
                                    │
                                    ▼
                           19 análisis calculados
                                    │
                                    ▼
                              MongoDB (resultados)
                                    │
                                    ▼
                              Superset (dashboard)
```

Spark nunca escribe datos crudos. Solo escribe resultados de análisis.

---

## Cómo está organizada la carpeta `spark/`

```
spark/
├── jobs/
│   ├── verify_cluster.py    # Sprint 1.1: confirma que el cluster lee los Parquet
│   └── example_queries.py  # Sprint 1.2: consultas de ejemplo sobre datos reales
└── README.md               # este archivo
```

Cada análisis de los sprints futuros (RF-12-01 a RF-12-19) vivirá como un archivo `.py` separado dentro de `jobs/`.

---

## Conceptos clave para entender el código

### DataFrame

La unidad de trabajo en Spark es el **DataFrame**: una tabla con filas y columnas, igual que un CSV o una hoja de Excel, pero distribuida en memoria entre todos los workers. Cuando el código hace `spark.read.parquet(...)`, Spark carga el archivo y lo representa como un DataFrame.

### Transformaciones vs acciones

Spark trabaja de forma **lazy**: las transformaciones (filtrar, agrupar, calcular columnas) no se ejecutan inmediatamente. Solo cuando se llama a una **acción** como `.show()`, `.count()` o `.write` es que Spark planifica y ejecuta el trabajo real. Esto permite que Spark optimice el plan de ejecución antes de tocar los datos.

Ejemplos:
- Transformaciones: `.filter()`, `.groupBy()`, `.select()`, `.join()`, `.withColumn()`
- Acciones: `.show()`, `.count()`, `.collect()`, `.write.parquet()`

### Las tres tablas GDELT

| Tabla | Archivo | Contenido principal |
|---|---|---|
| Events | `events.parquet` | Un evento por fila: quién hizo qué a quién, dónde, cuándo. Columnas clave: `goldsteinscale`, `avgtone`, `actor1countrycode`, `actor2countrycode`, `quadclass`, `eventcode`. |
| Mentions | `mentions.parquet` | Cada vez que un medio menciona un evento. Columnas clave: `globaleventid`, `mentionsourcename`, `mentiondoctone`. Se une a Events por `globaleventid`. |
| GKG | `gkg.parquet` | Contexto enriquecido: temas, organizaciones, personas, ubicaciones. Columnas clave: `themes`, `organizations`, `persons`, `v2tone`. Campos multi-valor separados por `;`. |

### Escala Goldstein

Va de −10 (máximo conflicto) a +10 (máxima cooperación). Es la métrica principal para medir la intensidad de un evento. Se usa en RF-12-01 (mapa de calor) y RF-12-08 (pares de países en conflicto).

### QuadClass (CAMEO)

Clasifica el tipo de interacción en 4 categorías:
- `1` = Cooperación verbal
- `2` = Cooperación material  
- `3` = Conflicto verbal
- `4` = Conflicto material

### AvgTone

Tono promedio del artículo. Negativo = cobertura negativa/conflictiva, positivo = cobertura positiva. Se usa en RF-12-03, RF-12-07 y RF-12-13.

---

## Cómo correr un job manualmente

Desde la raíz del repositorio, con el cluster corriendo:

```bash

docker-compose up -d

# Verificar que el cluster lee los Parquet (Sprint 1)
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-jobs/verify_cluster.py

# Correr las consultas de ejemplo (Sprint 1)
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-jobs/example_queries.py
```

La Spark Web UI muestra cada job en tiempo real: `http://localhost:8080`.

---

## Cómo Airflow va a invocar Spark (sprints futuros)

Airflow usará `SparkSubmitOperator` apuntando al master en `spark://spark-master:7077`. Cada tarea del DAG corresponderá a un archivo `.py` en `spark/jobs/`. No es necesario hacer nada extra en el cluster para que esto funcione: el puerto `7077` ya está expuesto en el `docker-compose.yml`.

---

## Límites conocidos de esta configuración

- Con 8 GB de RAM en el host y dos workers de 1 GB cada uno, los análisis que hagan joins grandes entre Events y Mentions pueden ser lentos. Si un job tarda más de 15 minutos, se puede aumentar `SPARK_WORKER_MEMORY` en el `docker-compose.yml`.
- Esta configuración usa `file://` para leer Parquet. Cuando el equipo de infraestructura levante HDFS, el único cambio en los jobs es reemplazar el `DATA_PATH` de `/opt/spark-data` a `hdfs://namenode:9000/gdelt/raw/<timestamp>/`.


# TL ; DR
```bash 
docker-compose up -d

# verificar cluster (verify_cluster.py)
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-jobs/verify_cluster.py

# correr queries (example_queries)
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-jobs/example_queries.py

# dónde está instalado Spark dentro del contenedor?: /opt/spark/bin/spark-submit
```
