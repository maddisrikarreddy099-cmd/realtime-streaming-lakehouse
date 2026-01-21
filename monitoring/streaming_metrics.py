from prometheus_client import Counter, Gauge, start_http_server
import time

EVENTS_PROCESSED = Counter(
    "events_processed_total",
    "Total events processed by streaming pipeline"
)

PIPELINE_LAG = Gauge(
    "pipeline_lag_seconds",
    "End-to-end pipeline latency"
)

def main():
    start_http_server(8000)  # exposes /metrics

    while True:
        EVENTS_PROCESSED.inc(5)
        PIPELINE_LAG.set(1.2)
        time.sleep(5)

if __name__ == "__main__":
    main()
