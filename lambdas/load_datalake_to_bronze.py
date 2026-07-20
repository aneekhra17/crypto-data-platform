"""
Lambda: data lake -> bronze

Triggered by S3 ObjectCreated events on the data lake bucket (both the
kafka/ and kinesis/ prefixes). Normalizes both sources into the same
one-file-per-coin-per-hour Bronze structure, regardless of how the raw
data was shaped on the way in:
  - Kafka-sourced files: exactly one JSON object per file
  - Kinesis Firehose-sourced files: one OR MORE newline-delimited JSON
    objects, batched together by Firehose's own buffering -- these get
    split out into individual records here, same as the Kafka path.

This is deliberately where normalization happens, not upstream --
raw/landing zones commonly keep source-specific quirks (that's normal),
Bronze is where consumers expect one consistent shape.
"""
import json
import os
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3

s3 = boto3.client("s3")
BRONZE_BUCKET = os.environ["BRONZE_BUCKET"]

TEST_COIN_ID_PREFIXES = ("test-coin-", "zzz-test-validation-")


def handler(event, context):
    processed = 0
    skipped = 0
    for record in event["Records"]:
        source_bucket = record["s3"]["bucket"]["name"]
        source_key = unquote_plus(record["s3"]["object"]["key"])

        obj = s3.get_object(Bucket=source_bucket, Key=source_key)
        raw_body = obj["Body"].read().decode("utf-8")

        ingestion_path = "kafka" if source_key.startswith("kafka/") else "kinesis"

        for line in raw_body.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                print(f"Skipping unparseable line in {source_key}")
                continue

            coin_id = item.get("coin_id", "")
            if coin_id.startswith(TEST_COIN_ID_PREFIXES):
                print(f"Skipping synthetic test record: {source_key} (coin_id={coin_id})")
                skipped += 1
                continue

            item["_ingestion_path"] = ingestion_path
            item["_loaded_at"] = datetime.now(timezone.utc).isoformat()

            now = datetime.now(timezone.utc)
            bronze_key = f"crypto-prices/{now:%Y-%m-%d}/{now.hour}/{coin_id}_000.json"

            s3.put_object(
                Bucket=BRONZE_BUCKET,
                Key=bronze_key,
                Body=json.dumps(item).encode("utf-8"),
            )
            print(f"Loaded {source_key} -> s3://{BRONZE_BUCKET}/{bronze_key}")
            processed += 1

    return {"statusCode": 200, "body": f"Processed {processed} record(s), skipped {skipped} test record(s)"}