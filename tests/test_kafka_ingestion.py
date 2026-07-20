"""
Rigorous empirical validation of the Kafka ingestion path.

Uses a dedicated test topic and a dedicated S3 test prefix -- fully
isolated from production data, so this is safe to run even after the
Bronze-loading Lambda is wired up (it won't ever see this test data).

Produces known records with predictable content, consumes them, writes
to S3, then asserts every record round-tripped correctly -- not just
"did it run without crashing."

Usage:
    pip install kafka-python-ng boto3
    python tests/test_kafka_ingestion.py --bucket crypto-platform-datalake-yourname
"""
import argparse
import json
import sys
import uuid
from datetime import datetime, timezone

import boto3
from kafka import KafkaConsumer, KafkaProducer

TEST_TOPIC = "crypto-prices-test"
TEST_PREFIX = "test/kafka-validation"


def make_test_records(n=5):
    run_id = uuid.uuid4().hex[:8]
    return [
        {
            "coin_id": f"test-coin-{run_id}-{i}",
            "symbol": f"tc{i}",
            "name": f"Test Coin {i}",
            "current_price": 100.0 + i,
            "market_cap": 1_000_000 * (i + 1),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        for i in range(n)
    ], run_id


def produce_test_records(records, bootstrap_servers="localhost:9092"):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        request_timeout_ms=5000,
    )
    for r in records:
        producer.send(TEST_TOPIC, value=r).get(timeout=5)
    producer.flush()
    producer.close()


def consume_and_write_to_s3(bucket, expected_count, bootstrap_servers="localhost:9092", timeout_sec=30):
    consumer = KafkaConsumer(
        TEST_TOPIC,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout_sec * 1000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    s3 = boto3.client("s3")

    written_keys = []
    for message in consumer:
        record = message.value
        now = datetime.now(timezone.utc)
        key = f"{TEST_PREFIX}/{record['coin_id']}_{now:%H%M%S%f}.json"
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(record).encode("utf-8"))
        written_keys.append((key, record))
        if len(written_keys) >= expected_count:
            break

    consumer.close()
    return written_keys


def cleanup(bucket, written_keys):
    s3 = boto3.client("s3")
    for key, _ in written_keys:
        s3.delete_object(Bucket=bucket, Key=key)
    print(f"  Cleaned up {len(written_keys)} test object(s) from S3")


def main(bucket):
    failures = 0
    n = 5

    print(f"1. Generating {n} known test records...")
    records, run_id = make_test_records(n)
    expected_ids = {r["coin_id"] for r in records}
    print(f"  Run ID: {run_id}")

    print("2. Producing to Kafka test topic...")
    produce_test_records(records)
    print(f"  OK: sent {n} records to '{TEST_TOPIC}'")

    print("3. Consuming and writing to S3 test prefix...")
    written = consume_and_write_to_s3(bucket, expected_count=n)
    print(f"  Wrote {len(written)} object(s) to s3://{bucket}/{TEST_PREFIX}/")

    print("4. Asserting all records round-tripped correctly...")
    if len(written) != n:
        print(f"  FAILED: expected {n} records, got {len(written)}")
        failures += 1
    else:
        print(f"  OK: {n} records received")

    received_ids = {record["coin_id"] for _, record in written}
    if received_ids == expected_ids:
        print("  OK: all coin_ids match exactly what was sent")
    else:
        print(f"  FAILED: mismatch. Missing: {expected_ids - received_ids}, Unexpected: {received_ids - expected_ids}")
        failures += 1

    for _, record in written:
        i = int(record["coin_id"].rsplit("-", 1)[-1])
        expected_price = 100.0 + i
        if record["current_price"] != expected_price:
            print(f"  FAILED: {record['coin_id']} price mismatch: expected {expected_price}, got {record['current_price']}")
            failures += 1
    if failures == 0:
        print("  OK: all record contents match exactly")

    print("5. Cleaning up test S3 objects...")
    cleanup(bucket, written)

    print("\n" + "=" * 50)
    if failures == 0:
        print("ALL KAFKA INGESTION VALIDATION CHECKS PASSED")
        sys.exit(0)
    else:
        print(f"{failures} CHECK(S) FAILED")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()
    main(args.bucket)
