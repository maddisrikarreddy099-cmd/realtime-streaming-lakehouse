import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any

# Uses confluent-kafka (recommended). Install:
#   pip install confluent-kafka
from confluent_kafka import Producer


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_event(event_type: str, source: str) -> Dict[str, Any]:
    """
    Event envelope matches docs/event_schema.json:
      - event_id: string (uuid)
      - event_type: string
      - event_time: timestamp (ISO-8601)
      - source: string
      - payload: string (JSON string)
      - ingestion_time: timestamp (ISO-8601)
    """
    payload_obj = {
        "user_id": f"user_{uuid.uuid4().hex[:8]}",
        "amount": round(10 + (uuid.uuid4().int % 5000) / 100, 2),
        "currency": "USD",
        "item_id": f"sku_{uuid.uuid4().hex[:6]}",
    }

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": utc_now_iso(),
        "source": source,
        "payload": json.dumps(payload_obj),        # payload is a JSON string
        "ingestion_time": utc_now_iso(),
    }


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def main():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "events")
    source = os.getenv("EVENT_SOURCE", "sample-producer")
    event_type = os.getenv("EVENT_TYPE", "order_created")
    num_events = int(os.getenv("NUM_EVENTS", "10"))
    interval_sec = float(os.getenv("INTERVAL_SEC", "0.5"))

    # Producer config
    conf = {
        "bootstrap.servers": bootstrap,
        # Good defaults for reliability in demos
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
        "compression.type": "snappy",
    }

    producer = Producer(conf)

    print("---- Producer Config ----")
    print(f"bootstrap: {bootstrap}")
    print(f"topic:     {topic}")
    print(f"source:    {source}")
    print(f"type:      {event_type}")
    print(f"count:     {num_events}")
    print(f"interval:  {interval_sec}s")
    print("-------------------------")

    for i in range(num_events):
        event = build_event(event_type=event_type, source=source)
        key = event["event_id"]  # good key for partitioning + traceability

        producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(event),
            on_delivery=delivery_report,
        )
        producer.poll(0)  # triggers delivery callbacks

        print(f"Sent {i+1}/{num_events}: event_id={event['event_id']}")
        time.sleep(interval_sec)

    producer.flush(10)
    print("✅ Done. Flushed outstanding messages.")


if __name__ == "__main__":
    main()

