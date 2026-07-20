"""
Consumes crypto price events from the local Kafka topic and lands them,
unmodified, in the S3 data lake bucket -- one file per coin per hour
(kafka/crypto-prices/{date}/{hour}/{coin_id}_000.json), overwritten on
any repeat write within the same hour rather than accumulating one file
per poll. The Kinesis Firehose fallback path delivers to S3 on its own
under a separate kinesis/ prefix (no custom consumer needed there).

Usage:
    pip install kafka-python-ng boto3

    # Continuous mode (manual/local use, runs forever):
    python crypto_consumer.py --bucket crypto-platform-datalake

    # Run-once mode (for scheduler-driven runs, e.g. Airflow): drains
    # whatever's currently in the topic, then exits once no new
    # messages arrive for --idle-timeout-sec seconds.
    python crypto_consumer.py --bucket crypto-platform-datalake --run-once
"""
import argparse
import json
import time
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer


def build_key(record, now):
    coin_id = record.get("coin_id", "unknown")
    return f"kafka/crypto-prices/{now:%Y-%m-%d}/{now.hour}/{coin_id}_000.json"


def run_consumer_loop(bootstrap_servers, topic, bucket, region, run_once=False, idle_timeout_sec=15):
    consumer_kwargs = dict(
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="crypto-datalake-writer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        api_version_auto_timeout_ms=10000,
    )
    if run_once:
        consumer_kwargs["consumer_timeout_ms"] = idle_timeout_sec * 1000

    consumer = KafkaConsumer(topic, **consumer_kwargs)
    s3 = boto3.client("s3", region_name=region)

    print(f"Consuming from Kafka topic '{topic}', writing to s3://{bucket}/kafka/crypto-prices/")

    count = 0
    for message in consumer:
        record = message.value
        now = datetime.now(timezone.utc)
        key = build_key(record, now)
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(record).encode("utf-8"))
        print(f"  Wrote s3://{bucket}/{key}")
        count += 1

    if run_once:
        print(f"Run-once mode: no new messages for {idle_timeout_sec}s, drained {count} record(s), exiting.")


def main(bootstrap_servers, topic, bucket, region, run_once=False, idle_timeout_sec=15):
    if run_once:
        run_consumer_loop(bootstrap_servers, topic, bucket, region, run_once=True, idle_timeout_sec=idle_timeout_sec)
        return

    backoff = 3
    while True:
        try:
            run_consumer_loop(bootstrap_servers, topic, bucket, region)
        except KeyboardInterrupt:
            print("Stopped by user.")
            return
        except Exception as e:
            print(f"Consumer crashed ({type(e).__name__}: {e}). Reconnecting in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="crypto-prices")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--run-once", action="store_true",
                         help="Drain current messages then exit, instead of running forever")
    parser.add_argument("--idle-timeout-sec", type=int, default=15,
                         help="In --run-once mode, exit after this many seconds with no new messages")
    args = parser.parse_args()
    main(args.bootstrap_servers, args.topic, args.bucket, args.region, args.run_once, args.idle_timeout_sec)
