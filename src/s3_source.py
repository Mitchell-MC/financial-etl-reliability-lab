"""
s3_source.py — Download source CSV files from S3 to the local raw landing zone.

Usage (standalone):
    python -m src.s3_source --bucket my-finance-bucket --prefix feeds/daily/

Usage (in pipeline, set env vars):
    S3_BUCKET=my-finance-bucket S3_PREFIX=feeds/daily/ python -m src.run_pipeline --source s3

Design notes:
  - Each S3 object matching SOURCE_KEY_MAP is downloaded to RAW_DIR, replacing the local file.
  - Uses presigned-URL-free approach: boto3 get_object with streaming chunked download.
  - Credentials are never hardcoded; resolved via the standard boto3 chain:
      1. Environment variables (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)
      2. ~/.aws/credentials profile
      3. EC2 / ECS / Lambda instance metadata (IAM role)
  - The --dry-run flag lists matching objects without downloading (safe for interview demos
    without live AWS credentials).
"""

import argparse
import os
from pathlib import Path

from src.config import RAW_DIR, SOURCE_FILES

# Maps local source name → expected S3 object key suffix.
# e.g. bucket/prefix/source_a_transactions.csv → "source_a"
SOURCE_KEY_MAP: dict[str, str] = {
    source_name: Path(local_path).name
    for source_name, local_path in SOURCE_FILES.items()
}


def download_sources(bucket: str, prefix: str = "", dry_run: bool = False) -> dict[str, str]:
    """
    Download all known source CSVs from S3 to RAW_DIR.

    Returns a dict mapping source_name → local file path for each file fetched.
    Raises ImportError if boto3 is not installed.
    Raises FileNotFoundError if an expected S3 key is missing (unless dry_run).
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise ImportError(
            "boto3 is required for S3 ingest. "
            "Install with: pip install -r requirements-aws.txt"
        ) from exc

    s3 = boto3.client("s3")
    downloaded: dict[str, str] = {}

    for source_name, filename in SOURCE_KEY_MAP.items():
        key = f"{prefix.rstrip('/')}/{filename}" if prefix else filename
        local_path = RAW_DIR / filename

        if dry_run:
            print(f"  [dry-run] would download s3://{bucket}/{key}  →  {local_path}")
            downloaded[source_name] = str(local_path)
            continue

        try:
            print(f"  Downloading s3://{bucket}/{key}  →  {local_path}")
            RAW_DIR.mkdir(parents=True, exist_ok=True)

            response = s3.get_object(Bucket=bucket, Key=key)
            with open(local_path, "wb") as f:
                for chunk in response["Body"].iter_chunks(chunk_size=65536):
                    f.write(chunk)

            size_kb = local_path.stat().st_size / 1024
            print(f"    ✓ {filename}  ({size_kb:.1f} KB)")
            downloaded[source_name] = str(local_path)

        except ClientError as exc:
            error_code = exc.response["Error"]["Code"]
            if error_code == "NoSuchKey":
                raise FileNotFoundError(
                    f"S3 key not found: s3://{bucket}/{key}"
                ) from exc
            raise

    return downloaded


def list_landing_objects(bucket: str, prefix: str = "") -> list[str]:
    """Return all object keys under the given S3 prefix (for diagnostics)."""
    try:
        import boto3
    except ImportError as exc:
        raise ImportError("boto3 required. pip install -r requirements-aws.txt") from exc

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def _cli():
    parser = argparse.ArgumentParser(
        description="Download source CSVs from S3 to the raw landing zone."
    )
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--prefix", default="", help="S3 key prefix (folder path)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be downloaded without fetching",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all objects under the prefix and exit",
    )
    args = parser.parse_args()

    if args.list:
        keys = list_landing_objects(args.bucket, args.prefix)
        print(f"\nObjects in s3://{args.bucket}/{args.prefix}")
        for k in keys:
            print(f"  {k}")
        return

    print(f"\nS3 Source Fetch — bucket={args.bucket}  prefix='{args.prefix}'")
    if args.dry_run:
        print("  (dry-run mode — no files will be written)\n")

    result = download_sources(args.bucket, args.prefix, dry_run=args.dry_run)
    print(f"\nReady to ingest: {list(result.keys())}")


if __name__ == "__main__":
    _cli()
