from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
def region_mundo(col):
    return (
        F.when(col.isin("USA", "CAN", "MEX", "GTM", "HND", "SLV", "NIC", "CRI", "PAN", "CUB", "DOM", "HTI", "JAM"), "Norte/Centroamérica y Caribe")
        .when(col.isin("BRA", "ARG", "CHL", "COL", "PER", "VEN", "ECU", "BOL", "PRY", "URY"), "Sudamérica")
        .when(col.isin("GBR", "FRA", "DEU", "ESP", "ITA", "PRT", "NLD", "BEL", "CHE", "AUT", "POL", "UKR", "RUS", "SWE", "NOR", "FIN", "DNK"), "Europa")
        .when(col.isin("CHN", "JPN", "KOR", "PRK", "IND", "PAK", "BGD", "IDN", "PHL", "VNM", "THA", "MYS", "SGP"), "Asia")
        .when(col.isin("IRN", "IRQ", "ISR", "PSE", "JOR", "LBN", "SYR", "SAU", "YEM", "ARE", "QAT", "KWT", "OMN", "TUR"), "Medio Oriente")
        .when(col.isin("ZAF", "NGA", "KEN", "ETH", "EGY", "SDN", "SSD", "MAR", "DZA", "TUN", "GHA", "UGA", "TZA", "COD", "CMR"), "África")
        .when(col.isin("AUS", "NZL", "FJI", "PNG"), "Oceanía")
        .otherwise("Otra/No clasificada")
    )
# 1. Mapa de calor de intensidad de conflictos
mapa = (
    events
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .filter(F.col("sqldate").isNotNull())
    .filter(F.col("goldsteinscale").isNotNull())
    .filter(F.col("goldsteinscale") < 0)
    .groupBy(F.col("sqldate").alias("fecha"),F.col("actiongeo_countrycode").alias("pais"))
    .agg(F.avg("goldsteinscale").alias("intensidad_promedio_conflicto"), F.count("*").alias("total_eventos_conflicto"))
    .orderBy("fecha", "pais")
)

guardar(mapa, "heatmap_conflictos")

# 2. Top 10 países que generan más eventos
eventos_por_pais_dia = (
    events
    .filter(F.col("sqldate").isNotNull())
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .groupBy(F.col("sqldate").alias("fecha"),F.col("actiongeo_countrycode").alias("pais"))
    .agg(F.count("*").alias("total_eventos_noticiosos"))
)
w = Window.partitionBy("fecha").orderBy(F.desc("total_eventos_noticiosos"))
top10_paises_dia = (
    eventos_por_pais_dia
    .withColumn("ranking", F.row_number().over(w))
    .filter(F.col("ranking") <= 10)
    .orderBy("fecha", "ranking")
)

guardar(top10_paises_dia, "top10_paises_eventos_dia")

# 3. Correlación AvgTone y número de fuentes
corr = (
    events
    .filter(F.col("avgtone").isNotNull())
    .filter(F.col("numsources").isNotNull())
    .agg(
        F.corr("avgtone", "numsources").alias("correlacion_avgtone_fuentes"),
        F.count("*").alias("total_eventos_analizados"),
        F.avg("avgtone").alias("promedio_avgtone"),
        F.avg("numsources").alias("promedio_fuentes")
    )
)

guardar(corr, "correlacion_avgtone_fuentes")

# 4. Distribución de tipos CAMEO
regiones = (
    spark.read
    .option("header", True)
    .csv("/opt/spark-static/country_regions.csv")
)

cameo_region = (
    events
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .filter(F.col("eventrootcode").isNotNull())
    .join(regiones,events.actiongeo_countrycode == regiones.country_code, "left")
    .withColumn( "region_mundo",F.coalesce(F.col("region_mundo"), F.lit("No clasificada")))
    .groupBy("region_mundo","eventrootcode")
    .agg(F.count("*").alias("total_eventos"))
)

guardar(cameo_region, "cameo_region_mundo")

# 5. Matriz de interacción entre actores
tipos_actores = [
    "GOV", "MIL", "REB",
    "OPP", "COP", "MED",
    "BUS", "CVL", "NGO"
]

matriz_actores = (
    events
    .filter(F.col("actor1type1code").isin(tipos_actores))
    .filter(F.col("actor2type1code").isin(tipos_actores))
    .groupBy(F.col("actor1type1code").alias("actor_origen"),F.col("actor2type1code").alias("actor_destino"))
    .agg( F.count("*").alias("frecuencia_interaccion"))
    .orderBy("actor_origen", "actor_destino")
)

guardar(matriz_actores, "matriz_interaccion_actores")

# 6. Cobertura mediática por país
cobertura_mediatica = (
    events
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .filter(F.col("nummentions").isNotNull())
    .groupBy( F.col("actiongeo_countrycode").alias("pais"))
    .agg( F.count("*").alias("total_eventos"),F.sum("nummentions").alias("total_menciones"),(F.sum("nummentions") / F.count("*")).alias("razon_menciones_por_evento"))
    .orderBy(F.desc("razon_menciones_por_evento"))
)

guardar(cobertura_mediatica, "cobertura_mediatica_pais")

# 7. Tendencia de sentimiento
tendencia_sentimiento = (
    events
    .filter(F.col("sqldate").isNotNull())
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .filter(F.col("avgtone").isNotNull())
    .groupBy(
        F.col("sqldate").alias("fecha"),
        F.col("actiongeo_countrycode").alias("pais")
    )
    .agg(
        F.avg("avgtone").alias("avg_tone_ventana"),
        F.count("*").alias("total_eventos")
    )
    .orderBy("pais", "fecha")
)

guardar(tendencia_sentimiento, "tendencia_sentimiento_pais")

# 8. Conflictos entre actores
conflictos_paises = (
    events
    .filter(F.col("actor1countrycode").isNotNull())
    .filter(F.col("actor2countrycode").isNotNull())
    .filter(F.col("actor1countrycode") != F.col("actor2countrycode"))
    .filter(F.col("goldsteinscale").isNotNull())
    .filter(F.col("goldsteinscale") < 0)
    .withColumn("pais_a",F.least(F.col("actor1countrycode"), F.col("actor2countrycode")) )
    .withColumn("pais_b",F.greatest(F.col("actor1countrycode"), F.col("actor2countrycode")))
    .groupBy("pais_a", "pais_b")
    .agg(F.count("*").alias("frecuencia_conflicto"),F.avg("goldsteinscale").alias("intensidad_promedio_goldstein"),F.min("goldsteinscale").alias("mayor_intensidad_conflicto"))
    .orderBy(F.desc("frecuencia_conflicto"))
)

guardar(conflictos_paises, "conflictos_pares_paises")

# 9. Escalada de eventos
escalada_eventos = (
    mentions
    .filter(F.col("globaleventid").isNotNull())
    .filter(F.col("mentiontimedate").isNotNull())
    .withColumn(
        "timestamp_mencion",
        F.to_timestamp(F.col("mentiontimedate").cast("string"), "yyyyMMddHHmmss")
    )
    .withColumn("hora", F.date_trunc("hour", F.col("timestamp_mencion")))
    .groupBy("globaleventid", "hora")
    .agg(F.count("*").alias("menciones_en_hora"))
    .orderBy(F.desc("menciones_en_hora"))
)

guardar(escalada_eventos, "escalada_eventos_1h")

# 10. Eventos basados en religión
religion_actor1 = (
    events
    .withColumn("goldstein_num", F.expr("try_cast(goldsteinscale as double)"))
    .filter(F.col("goldstein_num").isNotNull())
    .filter(F.col("goldstein_num") < 0)
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .filter(F.col("actor1religion1code").isNotNull())
    .select(
        F.col("actiongeo_countrycode").alias("country_code"),
        F.col("actor1religion1code").alias("religion"),
        F.col("goldstein_num").alias("goldsteinscale")
    )
)

religion_actor2 = (
    events
    .withColumn("goldstein_num", F.expr("try_cast(goldsteinscale as double)"))
    .filter(F.col("goldstein_num").isNotNull())
    .filter(F.col("goldstein_num") < 0)
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .filter(F.col("actor2religion1code").isNotNull())
    .select(
        F.col("actiongeo_countrycode").alias("country_code"),
        F.col("actor2religion1code").alias("religion"),
        F.col("goldstein_num").alias("goldsteinscale")
    )
)

religion_conflictos = religion_actor1.unionByName(religion_actor2)

religion_region = (
    religion_conflictos
    .join(regiones, "country_code", "left")
    .withColumn("region_mundo", F.coalesce(F.col("region_mundo"), F.lit("No clasificada")))
    .groupBy("region_mundo", "religion")
    .agg( F.count("*").alias("total_eventos_conflicto"),F.avg("goldsteinscale").alias("intensidad_promedio_conflicto"))
    .orderBy("region_mundo", F.desc("total_eventos_conflicto"))
)

guardar(religion_region, "conflictos_religion_region")

# 11. Temas GKG
gkg_temas = (
    gkg
    .filter(F.col("themes").isNotNull())
    .filter(F.col("locations").isNotNull())
    .withColumn("tema", F.explode(F.split(F.col("themes"), ";")))
    .withColumn("location", F.explode(F.split(F.col("locations"), ";")))
    .withColumn("country_code", F.regexp_extract(F.col("location"), r"#([A-Z]{2})#", 1))
    .withColumn("anio", F.substring(F.col("date").cast("string"), 1, 4))
    .filter(F.col("tema") != "")
    .filter(F.col("country_code") != "")
)

temas_region = (
    gkg_temas
    .join(regiones, "country_code", "left")
    .withColumn("region_mundo",F.coalesce(F.col("region_mundo"), F.lit("No clasificada")))
    .groupBy("region_mundo", "anio","tema")
    .agg( F.count("*").alias("frecuencia"))
)

w = (
    Window
    .partitionBy("region_mundo", "anio")
    .orderBy(F.desc("frecuencia"))
)

top_temas = (
    temas_region
    .withColumn( "ranking", F.row_number().over(w) ) .filter(F.col("ranking") <= 10)
)
guardar(top_temas, "top_temas_continente_anio")

# 12. Organizaciones más mencionadas por día
organizaciones_dia = (
    gkg
    .filter(F.col("organizations").isNotNull())
    .withColumn( "organizacion",F.explode(F.split(F.col("organizations"), ";")))
    .withColumn("fecha", F.substring(F.col("date").cast("string"), 1, 8))
    .filter(F.col("organizacion") != "")
    .groupBy( "fecha","organizacion")
    .agg( F.count("*").alias("total_menciones"))
)

w_org = (
    Window
    .partitionBy("fecha")
    .orderBy(F.desc("total_menciones"))
)

top_organizaciones = (
    organizaciones_dia
    .withColumn("ranking", F.row_number().over(w_org)).filter(F.col("ranking") <= 10).orderBy("fecha", "ranking")
)

guardar(top_organizaciones, "top_organizaciones_global_dia")

# 13. Análisis de rezago
tono_conflicto_ventana = (
    events
    .filter(F.col("sqldate").isNotNull())
    .filter(F.col("avgtone").isNotNull())
    .filter(F.col("goldsteinscale").isNotNull())
    .groupBy(F.col("sqldate").alias("fecha"))
    .agg(
        F.avg("avgtone").alias("tono_promedio_ventana"),
        F.count(F.when(F.col("goldsteinscale") < 0, True)).alias("conflictos_ventana"),
        F.count("*").alias("total_eventos_ventana")
    )
)

correlacion_tono_conflicto = (
    tono_conflicto_ventana
    .agg(
        F.corr("tono_promedio_ventana", "conflictos_ventana").alias("correlacion_tono_conflictos_ventana"),
        F.count("*").alias("total_ventanas_analizadas")
    )
)

guardar(tono_conflicto_ventana, "tono_conflicto_ventana")
guardar(correlacion_tono_conflicto, "correlacion_tono_conflicto_ventana")

# 14. Grafo diplomático vs conflictos
grafo_interacciones = (
    events
    .filter(F.col("actor1countrycode").isNotNull())
    .filter(F.col("actor2countrycode").isNotNull())
    .filter(F.col("actor1countrycode") != F.col("actor2countrycode"))
    .filter(F.col("goldsteinscale").isNotNull())
    .withColumn("pais_origen", F.col("actor1countrycode"))
    .withColumn( "pais_destino",F.col("actor2countrycode"))
    .withColumn("tipo_relacion",F.when( F.col("goldsteinscale") >= 0,"diplomatica").otherwise("conflicto"))
    .groupBy("pais_origen","pais_destino","tipo_relacion")
    .agg( F.count("*").alias("peso_arista"),F.avg("goldsteinscale").alias("goldstein_promedio"))
    .orderBy(F.desc("peso_arista"))
)

guardar(grafo_interacciones, "grafo_interacciones_paises")

# 15. Diversidad de fuentes por país
diversidad_fuentes = (
    mentions
    .filter(F.col("globaleventid").isNotNull())
    .filter(F.col("mentionsourcename").isNotNull())
    .join(events.select( "globaleventid",F.col("actiongeo_countrycode").alias("pais")),"globaleventid","inner")
    .filter(F.col("pais").isNotNull())
    .groupBy("pais")
    .agg(F.countDistinct("mentionsourcename").alias("medios_distintos"), F.count("*").alias("total_menciones"),F.countDistinct("globaleventid").alias("total_eventos"))
    .withColumn("indice_diversidad_fuentes",F.col("medios_distintos") / F.col("total_eventos"))
    .orderBy(F.desc("medios_distintos"))
)

guardar(diversidad_fuentes, "diversidad_fuentes_pais")

# 16. Frecuencia de conflictos por etnia
events_etnia = (
    events
    .withColumn(
        "goldstein_num",
        F.expr("try_cast(goldsteinscale as double)")
    )
)

etnias_actor1 = (
    events_etnia
    .filter(F.col("goldstein_num").isNotNull())
    .filter(F.col("goldstein_num") < 0)
    .filter(F.col("actor1ethniccode").isNotNull())
    .filter(F.col("actor1ethniccode") != "")
    .select(
        F.col("actor1ethniccode").alias("etnia"),
        F.col("goldstein_num").alias("goldsteinscale")
    )
)

etnias_actor2 = (
    events_etnia
    .filter(F.col("goldstein_num").isNotNull())
    .filter(F.col("goldstein_num") < 0)
    .filter(F.col("actor2ethniccode").isNotNull())
    .filter(F.col("actor2ethniccode") != "")
    .select(
        F.col("actor2ethniccode").alias("etnia"),
        F.col("goldstein_num").alias("goldsteinscale")
    )
)

etnias_conflicto = (
    etnias_actor1
    .unionByName(etnias_actor2)
    .groupBy("etnia")
    .agg(
        F.count("*").alias("frecuencia_conflictos"),
        F.avg("goldsteinscale").alias("intensidad_promedio_conflicto"),
        F.min("goldsteinscale").alias("maxima_intensidad_conflicto")
    )
    .orderBy(F.desc("frecuencia_conflictos"))
)

guardar(etnias_conflicto, "frecuencia_conflictos_etnia")

# 17. Noticias de última hora
menciones_por_evento_hora = (
    mentions
    .filter(F.col("globaleventid").isNotNull())
    .filter(F.col("mentiontimedate").isNotNull())
    .withColumn("timestamp_mencion",F.to_timestamp(F.col("mentiontimedate").cast("string"), "yyyyMMddHHmmss"))
    .withColumn("hora", F.date_trunc("hour", F.col("timestamp_mencion")))
    .groupBy("globaleventid", "hora")
    .agg(F.count("*").alias("menciones_en_hora"))
)

w_breaking = (
    Window
    .partitionBy("globaleventid")
    .orderBy("hora")
)

breaking_news = (
    menciones_por_evento_hora
    .withColumn("menciones_hora_anterior",F.coalesce(F.lag("menciones_en_hora").over(w_breaking),F.lit(0)))
    .filter(F.col("menciones_hora_anterior") == 0)
    .filter(F.col("menciones_en_hora") > 100)
    .orderBy(F.desc("menciones_en_hora"))
)

guardar(breaking_news, "breaking_news_ultima_hora")

# EXTRA 1. Distribución de eventos positivos y negativos

positivos_negativos = (
    events
    .filter(F.col("goldsteinscale").isNotNull())
    .withColumn("tipo_evento",F.when(F.col("goldsteinscale") >= 0, "Cooperacion").otherwise("Conflicto"))
    .groupBy("tipo_evento")
    .count()
)

guardar(positivos_negativos, "eventos_positivos_negativos")

# EXTRA 2. Top 10 países menos noticiosos

paises_menos_noticiosos = (
    events
    .filter(F.col("actiongeo_countrycode").isNotNull())
    .groupBy(F.col("actiongeo_countrycode").alias("pais"))
    .agg( F.count("*").alias("total_eventos"))
    .filter(F.col("total_eventos") > 0)
    .orderBy(F.asc("total_eventos"), F.asc("pais"))
    .limit(10)
)

guardar(paises_menos_noticiosos, "top10_paises_menos_noticiosos")

spark.stop()
