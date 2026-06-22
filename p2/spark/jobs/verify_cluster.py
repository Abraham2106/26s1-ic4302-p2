from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("gdelt-verify")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

paths = {
    "events": "/opt/spark-data/events.parquet",
    "mentions": "/opt/spark-data/mentions.parquet",
    "gkg": "/opt/spark-data/gkg.parquet",
}

for name, path in paths.items():
    print(f"\n===== {name.upper()} =====")
    df = spark.read.parquet(path)
    print("Filas:", df.count())
    print("Columnas:", df.columns)
    df.printSchema()
    if name == "events":
        print("\nCODIGOS DE PAIS:")
        df.select("actiongeo_countrycode").distinct().show(100, truncate=False)

spark.stop()