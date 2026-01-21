from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

BRONZE_PATH = "data/bronze/events"
SILVER_PATH = "data/silver/events"
CHECKPOINT_PATH = "data/checkpoints/silver_events"

spark = (
    SparkSession.builder
    .appName("SilverTransform")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("delta").load(BRONZE_PATH)

df = df.withWatermark("event_time", "10 minutes")

dedup = (
    df.dropDuplicates(["event_id"])
)

query = (
    dedup.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(SILVER_PATH)
)

query.awaitTermination()
