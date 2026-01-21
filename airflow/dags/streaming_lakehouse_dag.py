from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="streaming_lakehouse_pipeline",
    default_args=DEFAULT_ARGS,
    description="Orchestrates Kafka -> Spark -> Delta Bronze/Silver/Gold streaming jobs",
    schedule_interval=None,   # manual trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["streaming", "kafka", "spark", "delta"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command="""
        spark-submit \
          --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
          spark/streaming/bronze_ingestion.py
        """,
    )

    silver = BashOperator(
        task_id="silver_transform",
        bash_command="""
        spark-submit \
          --packages io.delta:delta-core_2.12:2.4.0 \
          spark/streaming/silver_transform.py
        """,
    )

    gold = BashOperator(
        task_id="gold_aggregations",
        bash_command="""
        spark-submit \
          --packages io.delta:delta-core_2.12:2.4.0 \
          spark/streaming/gold_aggregations.py
        """,
    )

    bronze >> silver >> gold
