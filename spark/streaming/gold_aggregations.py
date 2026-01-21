from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count

SILVER_PATH = "data/silver/events"
GOLD_PATH = "data/gold/events_kpis"
CHECKPOINT_PATH = "data/checkpoints/gold_events_kpis"

spark = (
    SparkSession.builder
    .appName("GoldAggregations")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("delta").load(SILVER_PATH)

# Watermark helps manage late-arriving events in streaming
df = df.withWatermark("event_time", "10 minutes")

kpis = (
    df.groupBy(
        window(col("event_time"), "1 minute").alias("event_window"),
        col("event_type")
    )
    .agg(count("*").alias("event_count"))
)

out = (
    kpis.select(
        col("event_window.start").alias("window_start"),
        col("event_window.end").alias("window_end"),
        col("event_type"),
        col("event_count")
    )
)

query = (
    out.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(GOLD_PATH)
)

query.awaitTermination()
