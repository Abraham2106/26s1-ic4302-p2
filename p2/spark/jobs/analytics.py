from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("gdelt-analytics")
    .config(
        "spark.mongodb.write.connection.uri",
        "mongodb://admin:admin123@mongodb:27017/gdelt?authSource=admin"
    )
    .getOrCreate()
)

events = spark.read.parquet("/opt/spark-data/events.parquet")
mentions = spark.read.parquet("/opt/spark-data/mentions.parquet")
gkg = spark.read.parquet("/opt/spark-data/gkg.parquet")

def guardar(df, coleccion):
    (
        df.write
        .format("mongodb")
        .mode("overwrite")
        .option("database", "gdelt")
        .option("collection", coleccion)
        .save()
    )

# 1. Mapa de calor de intensidad de conflictos
heatmap=(
    events
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .groupBy("sqldate","actiongeo_countrycode")
    .agg(F.avg("goldsteinscale").alias("promedio_goldstein"))
)

guardar(heatmap, "heatmap_goldstein")

# 2. Top 10 países que generan más eventos
top10=(
    events
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .groupBy("sqldate","actiongeo_countrycode" )
    .count()
)

guardar(top10, "top10_countries")

# 3. Correlación AvgTone y número de fuentes
corr=(
    events
    .select("globaleventid","avgtone","numsources")
)

guardar(corr, "avgtone_correlation")

# 4. Distribución de tipos CAMEO
cameo=(
    events
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .groupBy("actiongeo_countrycode","eventrootcode")
    .count()
)

guardar(cameo, "cameo_distribution")

# 5. Matriz de interacción entre actores
interacciones=(
    events
    .groupBy("actor1type1code","actor2type1code")
    .count()
)

guardar(interacciones, "actor_interactions")

# 6. Cobertura mediática por país
cobertura=(
    events
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .groupBy("actiongeo_countrycode")
    .agg(
        (
            F.sum("nummentions")
            /
            F.count("*")
        ).alias("ratio")
    )
)

guardar(cobertura, "media_coverage")

# 7. Tendencia de sentimiento
sentimiento = (
    events
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .groupBy("sqldate","actiongeo_countrycode")
    .agg(F.avg("avgtone").alias("avg_tone"))
)

guardar(sentimiento, "sentiment_trends")

# 8. Conflictos entre actores
conflictos = (
    events
    .filter(F.col("goldsteinscale") < 0)
    .groupBy("actor1countrycode","actor2countrycode")
    .count()
)

guardar(conflictos, "country_conflicts")

# 9. Escalada de eventos
escalada=(
    mentions
    .groupBy("globaleventid","mentiontimedate")
    .agg(F.count("*").alias("menciones"))
)

guardar(escalada, "event_escalation")

# 10. Eventos basados en religión
religion = (
    events
    .groupBy("actor1religion1code","actiongeo_countrycode")
    .count()
)

guardar(religion, "religion_clusters")

# 11. Temas GKG
temas=(
    gkg
    .select(
        F.explode(
            F.split(
                F.col("themes"),
                ";"
            )
        ).alias("tema")
    )
    .filter(F.col("tema") != "")
    .groupBy("tema")
    .count()
)

guardar(temas, "gkg_topics")

# 12. Organizaciones más mencionadas por día
orgs = (
    gkg
    .select(
        "date",
        F.explode(
            F.split(
                F.col("organizations"),
                ";"
            )
        ).alias("organizacion")
    )
    .filter(F.col("organizacion") != "")
    .groupBy("date","organizacion")
    .count()
)

guardar(orgs, "organizations")


# 13. Análisis de rezago
lag = (
    events
    .groupBy("sqldate")
    .agg(F.avg("avgtone").alias("avg_tone"),F.avg("goldsteinscale").alias("avg_goldstein"))
)

guardar(lag, "lag_analysis")

# 14. Grafo diplomático vs conflictos
grafo = (
    events
    .groupBy("actor1countrycode","actor2countrycode")
    .agg(F.count("*").alias("peso"))
)

guardar(grafo, "diplomatic_graph")

# 15. Diversidad de fuentes por país
fuentes = (
    mentions
    .join(events.select("globaleventid","actiongeo_countrycode"),"globaleventid")
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .groupBy("actiongeo_countrycode")
    .agg(F.countDistinct("mentionsourcename").alias("fuentes_distintas"))
)

guardar(fuentes, "source_diversity")

# 16. Frecuencia de conflictos por etnia
etnias = (
    events
    .groupBy("actor1ethniccode")
    .count()
)

guardar(etnias, "ethnic_conflicts")

# 17. Noticias de última hora
breaking = (
    mentions
    .groupBy("globaleventid","mentiontimedate")
    .agg(F.count("*").alias("menciones"))
    .filter(F.col("menciones") >= 100)
)

guardar(breaking, "breaking_news")

# EXTRA 1. Top 10 países menos noticiosos
menos = (
    events
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .groupBy("actiongeo_countrycode")
    .count()
    .orderBy("count")
    .limit(10)
)

guardar(menos, "extra_analysis_1")

# EXTRA 2. Top 10 países más cooperativos
cooperativos = (
    events
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .groupBy("actiongeo_countrycode")
    .agg(F.avg("goldsteinscale").alias("promedio_goldstein"))
    .orderBy(F.desc("promedio_goldstein"))
    .limit(10)
)

guardar(cooperativos, "extra_analysis_2")
d
spark.stop()