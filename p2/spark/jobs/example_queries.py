from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("gdelt-queries-demo")
    .master("spark://spark-master:7077")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DATA_PATH = "/opt/spark-data"

events   = spark.read.parquet(f"{DATA_PATH}/events.parquet")
mentions = spark.read.parquet(f"{DATA_PATH}/mentions.parquet")
gkg      = spark.read.parquet(f"{DATA_PATH}/gkg.parquet")

# ── 1. Lectura y conteo básico ────────────────────────────────────────────────
print("\n[1] Conteo de filas por tabla")
for name, df in [("events", events), ("mentions", mentions), ("gkg", gkg)]:
    print(f"  {name}: {df.count()} filas")

# ── 2. Filtro simple ──────────────────────────────────────────────────────────
print("\n[2] Eventos con Goldstein < -5 (conflictos severos)")
severe = events.filter(F.col("goldsteinscale") < -5)
severe.select("globaleventid", "actor1countrycode", "actor2countrycode", "goldsteinscale").show()

# ── 3. Agregación: top países por cantidad de eventos ─────────────────────────
print("\n[3] Top países por número de eventos (actor1countrycode)")
(
    events
    .groupBy("actor1countrycode")
    .agg(F.count("*").alias("total_events"))
    .orderBy(F.desc("total_events"))
    .limit(10)
    .show()
)

# ── 4. Agregación: promedio de Goldstein y AvgTone por país ───────────────────
print("\n[4] Promedio Goldstein y AvgTone por país (actiongeo_countrycode)")
(
    events
    .groupBy("actiongeo_countrycode")
    .agg(
        F.avg("goldsteinscale").alias("avg_goldstein"),
        F.avg("avgtone").alias("avg_tone"),
        F.count("*").alias("n_events"),
    )
    .orderBy(F.asc("avg_goldstein"))
    .show()
)

# ── 5. Join events + mentions: menciones por evento ───────────────────────────
print("\n[5] Total de menciones por evento (join events + mentions)")
(
    events
    .join(mentions, on="globaleventid", how="left")
    .groupBy("globaleventid", "actor1countrycode", "actor2countrycode")
    .agg(F.sum("mentionnum").alias("total_mentions"))
    .orderBy(F.desc("total_mentions"))
    .show()
)

# ── 6. GKG: organizaciones más mencionadas ────────────────────────────────────
print("\n[6] Organizaciones más mencionadas (GKG)")
(
    gkg
    .select(F.explode(F.split(F.col("organizations"), ";")).alias("org"))
    .filter(F.col("org") != "")
    .groupBy("org")
    .agg(F.count("*").alias("mentions"))
    .orderBy(F.desc("mentions"))
    .limit(10)
    .show(truncate=False)
)

# ── 7. GKG: temas (themes) más frecuentes ────────────────────────────────────
print("\n[7] Temas más frecuentes (GKG themes)")
(
    gkg
    .select(F.explode(F.split(F.col("themes"), ";")).alias("theme"))
    .filter(F.col("theme") != "")
    .groupBy("theme")
    .agg(F.count("*").alias("freq"))
    .orderBy(F.desc("freq"))
    .limit(10)
    .show(truncate=False)
)

# ── 8. Distribución de cuadrantes CAMEO ──────────────────────────────────────
print("\n[8] Distribución de quadclass (tipo de interacción CAMEO)")
QUAD_LABELS = {1: "Verbal Cooperation", 2: "Material Cooperation", 3: "Verbal Conflict", 4: "Material Conflict"}
quad_df = (
    events
    .groupBy("quadclass")
    .agg(F.count("*").alias("count"))
    .orderBy("quadclass")
)
quad_df.show()

# ── 9. Ventana: ranking de eventos por tono dentro de cada país ───────────────
print("\n[9] Rank de eventos por avgtone dentro de cada país (Window)")
w = Window.partitionBy("actiongeo_countrycode").orderBy(F.asc("avgtone"))
(
    events
    .withColumn("rank_tone", F.rank().over(w))
    .select("actiongeo_countrycode", "globaleventid", "avgtone", "rank_tone")
    .filter(F.col("rank_tone") == 1)
    .show()
)

spark.stop()
