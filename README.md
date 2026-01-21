## Architecture

Kafka → Spark Structured Streaming → Delta Lake → Bronze / Silver / Gold

Bronze: raw Kafka JSON  
Silver: validated, deduped, typed  
Gold: analytics KPIs  

## Orchestration
Airflow DAG triggers all Spark jobs in order.

## Monitoring
Prometheus metrics exposed for:
- events processed
- pipeline lag

## Use Cases
- real-time order tracking
- payment monitoring
- fraud detection
- behavioral analytics
