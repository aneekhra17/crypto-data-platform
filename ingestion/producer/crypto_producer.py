"""
Polls CoinGecko's free, keyless public API for live crypto market data
and publishes each snapshot as an event.

Dual-path ingestion, by design (not because it's the "normal" production
pattern -- it's deliberately built to compare both platforms):
  1. PRIMARY: try to publish to local Kafka
  2. FALLBACK: if Kafka is unreachable, publish directly to Kinesis
     Firehose instead (same source, same eventual destination: S3)

Usage:
    pip install requests kafka-python-ng boto3
    python crypto_producer.py --interval-sec 60
"""
import argparse
import json
import time
from datetime import datetime, timezone

import boto3
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=market_cap_desc&per_page=20&page=1&sparkline=false"
)


def fetch_market_data():
    resp = requests.get(COINGECKO_URL, timeout=10)
    resp.raise_for_status()
    coins = resp.json()

    fetched_at = datetime.now(timezone.utc).isoformat()
    return [
        {
            "coin_id": c["id"],
            "symbol": c["symbol"],
            "name": c["name"],
            "current_price": c["current_price"],
            "market_cap": c["market_cap"],
            "market_cap_rank": c["market_cap_rank"],
            "total_volume": c["total_volume"],
            "price_change_24h": c.get("price_change_percentage_24h"),
            "fetched_at": fetched_at,
        }
        for c in coins
    ]


def get_kafka_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=5000,
        api_version_auto_timeout_ms=5000,
    )


def send_via_kafka(producer, topic, records):
    for record in records:
        future = producer.send(topic, value=record)
        future.get(timeout=5)  # raises on failure, forcing the fallback path
    producer.flush()


def send_via_kinesis_firehose(firehose_client, delivery_stream_name, records):
    for record in records:
        firehose_client.put_record(
            DeliveryStreamName=delivery_stream_name,
            Record={"Data": (json.dumps(record) + "\n").encode("utf-8")},
        )


def main(kafka_bootstrap, kafka_topic, firehose_stream, region, interval_sec):
    firehose_client = boto3.client("firehose", region_name=region)

    print(f"Starting crypto ingestion loop, polling every {interval_sec}s")
    print(f"  Primary:  Kafka ({kafka_bootstrap}, topic={kafka_topic})")
    print(f"  Fallback: Kinesis Firehose ({firehose_stream})")

    while True:
        try:
            records = fetch_market_data()
            print(f"[{datetime.now().isoformat()}] Fetched {len(records)} coins from CoinGecko")
        except requests.RequestException as e:
            print(f"  CoinGecko fetch failed: {e}. Skipping this cycle.")
            time.sleep(interval_sec)
            continue

        try:
            producer = get_kafka_producer(kafka_bootstrap)
            send_via_kafka(producer, kafka_topic, records)
            producer.close()
            print(f"  OK: sent {len(records)} records via Kafka (primary)")
        except (KafkaError, Exception) as e:
            print(f"  Kafka unavailable ({type(e).__name__}: {e}) -- falling back to Kinesis Firehose")
            try:
                send_via_kinesis_firehose(firehose_client, firehose_stream, records)
                print(f"  OK: sent {len(records)} records via Kinesis Firehose (fallback)")
            except Exception as fe:
                print(f"  FAILED on fallback path too: {fe}")

        time.sleep(interval_sec)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kafka-bootstrap", default="localhost:9092")
    parser.add_argument("--kafka-topic", default="crypto-prices")
    parser.add_argument("--firehose-stream", default="crypto-prices-fallback")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--interval-sec", type=int, default=60)
    args = parser.parse_args()
    main(args.kafka_bootstrap, args.kafka_topic, args.firehose_stream, args.region, args.interval_sec)
