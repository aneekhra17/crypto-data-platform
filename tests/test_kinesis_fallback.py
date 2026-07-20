"""
Rigorous empirical validation of the Kinesis Firehose fallback path.

Unlike the Kafka test, this one can't be fully isolated -- Firehose
delivers to a single fixed S3 destination, the same one production
uses. To avoid ever confusing this with real data, every test record
is tagged with a unique, obviously-fake coin_id, and the test cleans
up the S3 objects it creates at the end.

IMPORTANT: Firehose buffers before flushing to S3 (up to 5 minutes by
default), so this test genuinely takes a few minutes to run. That's
real Firehose behavior, not a bug -- the script polls S3 periodically
rather than giving up early.

Usage:
    pip install boto3
    python tests/test_kinesis_fallback.py --bucket crypto-platform-datalake-yourname --stream crypto-prices-fallback
"""
import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3

TEST_PREFIX_MARKER = "kinesis/crypto-prices/"  # Firehose's real delivery prefix


def make_test_records(n=5):
    run_id = uuid.uuid4().hex[:8]
    return [
        {
            "coin_id": f"zzz-test-validation-{run_id}-{i}",
            "symbol": f"zt{i}",
            "name": f"Kinesis Test Coin {i}",
            "current_price": 200.0 + i,
            "market_cap": 2_000_000 * (i + 1),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }
        for i in range(n)
    ], run_id


def send_to_firehose(records, stream_name, region="us-east-1"):
    client = boto3.client("firehose", region_name=region)
    for r in records:
        client.put_record(
            DeliveryStreamName=stream_name,
            Record={"Data": (json.dumps(r) + "\n").encode("utf-8")},
        )
    return client


def poll_for_delivery(bucket, run_id, expected_count, timeout_sec=360, poll_interval=15):
    s3 = boto3.client("s3")
    start = time.time()
    found = {}

    print(f"  Polling s3://{bucket}/{TEST_PREFIX_MARKER} for delivery (up to {timeout_sec}s, Firehose buffers before flushing)...")
    while time.time() - start < timeout_sec:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=TEST_PREFIX_MARKER):
            for obj in page.get("Contents", []):
                if obj["Key"] in found:
                    continue
                body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode("utf-8")
                for line in body.splitlines():
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if run_id in str(record.get("coin_id", "")):
                        found.setdefault(obj["Key"], []).append(record)

        total_records = sum(len(v) for v in found.values())
        elapsed = int(time.time() - start)
        print(f"    [{elapsed}s] Found {total_records}/{expected_count} test records so far...")
        if total_records >= expected_count:
            return found
        time.sleep(poll_interval)

    return found


def cleanup(bucket, found_keys):
    s3 = boto3.client("s3")
    for key in found_keys:
        s3.delete_object(Bucket=bucket, Key=key)
    print(f"  Cleaned up {len(found_keys)} test object(s) from S3")


def main(bucket, stream_name, region):
    failures = 0
    n = 5

    print(f"1. Generating {n} known test records...")
    records, run_id = make_test_records(n)
    expected_ids = {r["coin_id"] for r in records}
    print(f"  Run ID: {run_id}")

    print(f"2. Sending directly to Kinesis Firehose stream '{stream_name}'...")
    send_to_firehose(records, stream_name, region)
    print(f"  OK: sent {n} records via put_record")

    print("3. Waiting for Firehose to flush to S3 (this is the slow part)...")
    found = poll_for_delivery(bucket, run_id, expected_count=n)
    all_records = [r for records_list in found.values() for r in records_list]
    print(f"  Found {len(all_records)} matching record(s) across {len(found)} file(s)")

    print("4. Asserting all records were delivered correctly...")
    if len(all_records) != n:
        print(f"  FAILED: expected {n} records, got {len(all_records)}")
        failures += 1
    else:
        print(f"  OK: {n} records delivered")

    received_ids = {r["coin_id"] for r in all_records}
    if received_ids == expected_ids:
        print("  OK: all coin_ids match exactly what was sent")
    else:
        print(f"  FAILED: mismatch. Missing: {expected_ids - received_ids}")
        failures += 1

    print("5. Cleaning up test S3 objects...")
    cleanup(bucket, found.keys())

    print("\n" + "=" * 50)
    if failures == 0:
        print("ALL KINESIS FIREHOSE VALIDATION CHECKS PASSED")
        sys.exit(0)
    else:
        print(f"{failures} CHECK(S) FAILED")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--stream", default="crypto-prices-fallback")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()
    main(args.bucket, args.stream, args.region)
