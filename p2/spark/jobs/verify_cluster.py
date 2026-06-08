from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("gdelt-verify")
    .master("spark://spark-master:7077")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

DATA_PATH = "/opt/spark-data"
TABLES = ["events", "mentions", "gkg"]

for table in TABLES:
    path = f"{DATA_PATH}/{table}.parquet"
    df = spark.read.parquet(path)
    print(f"\n{'='*50}")
    print(f"TABLE: {table.upper()}")
    print(f"Rows : {df.count()}")
    print(f"Cols : {len(df.columns)}")
    df.printSchema()

spark.stop()
