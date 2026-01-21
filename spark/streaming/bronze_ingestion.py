import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import (
    col,
    from_json,
    expr,
    current_timestamp,
    to_timestamp,
    sha2,
    concat_ws
)

# ----------------------------
# Helpers
# ----------------------------
def load_spark_schema_from_json(spark: SparkSession, schema_path: str) -> StructType:
    """
    Loads a Spark StructType from the JSON file you stored in docs/event_schema.json.
    The JSON must be in the Spark StructType JSON format (type=struct, fields=[...]).
    """
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_dict = json.load(f)
    return StructType.fromJson(schema_dict)

# ----------------------------
# Main
# ----------------------------
def main():
    # ---- Config (set via env vars or defaults) ----
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "events")
    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", "latest")  # earliest|latest|json
    checkpoint_path = os.getenv("CHECKPOINT_PATH", "tmp/checkpoints/bronze_events")
    bronze_table_path = os.getenv("BRONZE_TABLE_PATH", "tmp/delta/bronze_events")

    # Location of your schema file in the repo
    schema_path = os.getenv("EVENT_SCHEMA_PATH", "docs/event_schema.json")

    spark = (
        SparkSession.builder
        .appName("bronze-ingestion-kafka-to-delta")
        # If you're using Delta locally, you may need delta configs later (we’ll add when you run)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # ---- Load schema and read Kafka ----
    event_schema = load_spark_schema_from_json(spark, schema_path)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    # Kafka value is bytes -> string -> JSON -> struct
    parsed = (
        kafka_df
        .withColumn("value_str", col("value").cast("string"))
        .withColumn("event", from_json(col("value_str"), event_schema))
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("event.*")
        )
        # Ensure ingestion_time exists even if producer didn’t send it
        .withColumn("ingestion_time", expr("coalesce(ingestion_time, current_timestamp())"))
        # Normalize event_time if it arrived as string in some producers (safe no-op if already timestamp)
        .withColumn("event_time", expr("coalesce(event_time, to_timestamp(event_time))"))
    )

    # ---- Idempotency key (dedupe support in downstream) ----
    # If event_id exists, use it. Otherwise hash key fields.
    enriched = (
        parsed
        .withColumn(
            "record_hash",
            sha2(
                concat_ws("||",
                    col("event_id"),
                    col("event_type"),
                    col("source"),
                    col("event_time").cast("string"),
                    col("payload")
                ),
                256
            )
        )
    )

    # ---- Watermark (event-time correctness) ----
    # This won’t drop duplicates by itself; it prepares for stateful ops later.
    with_watermark = enriched.withWatermark("event_time", "10 minutes")

    # ---- Write Bronze (Delta-style lakehouse landing) ----
    # For now we write to a path so it works locally; later we can switch to a managed Delta table.
    query = (
        with_watermark.writeStream
        .format("parquet")  # switch to "delta" when Delta is set up; parquet works everywhere
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start(bronze_table_path)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
