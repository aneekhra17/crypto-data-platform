"""
Consumes crypto price events from the local Kafka topic and lands them,
unmodified, in the S3 data lake bucket. This is the Kafka side of
ingestion -- the Kinesis Firehose fallback path delivers to S3 on its
own (no custom consumer needed for that path, Firehose does it natively).

Usage:
    pip install kafka-python-ng boto3
    python crypto_consumer.py --bucket crypto-platform-datalake-yourname
"""
import argparse
import json
import time
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer


def run_consumer_loop(bootstrap_servers, topic, bucket, region):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="crypto-datalake-writer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        api_version_auto_timeout_ms=10000,
    )
    s3 = boto3.client("s3", region_name=region)

    print(f"Consuming from Kafka topic '{topic}', writing to s3://{bucket}/kafka/crypto-prices/")

    for message in consumer:
        record = message.value
        now = datetime.now(timezone.utc)
        key = (
            f"kafka/crypto-prices/{now:%Y/%m/%d}/"
            f"{record.get('coin_id', 'unknown')}_{now:%H%M%S%f}.json"
        )
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(record).encode("utf-8"))
        print(f"  Wrote s3://{bucket}/{key}")


def main(bootstrap_servers, topic, bucket, region):
    # kafka-python has a known socket-reconnect race condition (upstream
    # issue, fixed in recent versions but can still surface transiently,
    # especially right after a broker restarts). Rather than let one bad
    # reconnect crash the whole consumer, retry with backoff.
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
            backoff = min(backoff * 2, 30)  # exponential backoff, capped at 30s


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="crypto-prices")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()
    main(args.bootstrap_servers, args.topic, args.bucket, args.region)
