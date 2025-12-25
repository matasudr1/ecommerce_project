"""
Upload Local Data to S3

This script uploads generated sample data to S3 for processing
by the AWS Glue pipeline.

Usage:
    python scripts/upload_to_s3.py --bucket your-bucket-name --source data/raw
    python scripts/upload_to_s3.py --bucket your-bucket-name --source data/raw --prefix raw/2024/01/01
"""

import os
import sys
import argparse
import logging
from pathlib import Path

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_s3_client(region: str = "eu-west-1"):
    """Get an S3 client with error handling."""
    try:
        return boto3.client("s3", region_name=region)
    except NoCredentialsError:
        logger.error(
            "AWS credentials not found. Please configure credentials:\n"
            "  1. Run 'aws configure' to set up credentials, or\n"
            "  2. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables, or\n"
            "  3. Use an IAM role if running on AWS infrastructure"
        )
        sys.exit(1)


def upload_file(s3_client, local_path: str, bucket: str, key: str) -> bool:
    """
    Upload a single file to S3.
    
    Args:
        s3_client: Boto3 S3 client
        local_path: Path to local file
        bucket: S3 bucket name
        key: S3 object key (path within bucket)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        s3_client.upload_file(local_path, bucket, key)
        return True
    except ClientError as e:
        logger.error(f"Failed to upload {local_path}: {e}")
        return False


def upload_directory(
    source_dir: str,
    bucket: str,
    prefix: str = "raw/",
    region: str = "eu-west-1"
) -> dict:
    """
    Upload all files from a directory to S3.
    
    Args:
        source_dir: Local directory containing files to upload
        bucket: S3 bucket name
        prefix: S3 prefix (folder path)
        region: AWS region
        
    Returns:
        Dictionary with upload statistics
    """
    s3 = get_s3_client(region)
    
    # Verify bucket exists
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "404":
            logger.error(f"Bucket '{bucket}' does not exist")
        elif error_code == "403":
            logger.error(f"Access denied to bucket '{bucket}'")
        else:
            logger.error(f"Error accessing bucket: {e}")
        sys.exit(1)
    
    # Get list of files to upload
    source_path = Path(source_dir)
    if not source_path.exists():
        logger.error(f"Source directory does not exist: {source_dir}")
        sys.exit(1)
    
    files = list(source_path.glob("**/*"))
    files = [f for f in files if f.is_file()]
    
    if not files:
        logger.warning(f"No files found in {source_dir}")
        return {"uploaded": 0, "failed": 0, "skipped": 0}
    
    logger.info(f"Found {len(files)} files to upload")
    
    stats = {"uploaded": 0, "failed": 0, "skipped": 0}
    
    for file_path in tqdm(files, desc="Uploading"):
        # Build S3 key
        relative_path = file_path.relative_to(source_path)
        s3_key = f"{prefix.rstrip('/')}/{relative_path}".replace("\\", "/")
        
        # Skip hidden files
        if any(part.startswith(".") for part in relative_path.parts):
            stats["skipped"] += 1
            continue
        
        # Upload
        if upload_file(s3, str(file_path), bucket, s3_key):
            stats["uploaded"] += 1
            logger.debug(f"Uploaded: {s3_key}")
        else:
            stats["failed"] += 1
    
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Upload local data to S3 for Glue processing"
    )
    parser.add_argument(
        "--bucket", "-b",
        required=True,
        help="S3 bucket name"
    )
    parser.add_argument(
        "--source", "-s",
        default="data/raw",
        help="Local source directory (default: data/raw)"
    )
    parser.add_argument(
        "--prefix", "-p",
        default="raw/",
        help="S3 prefix/folder (default: raw/)"
    )
    parser.add_argument(
        "--region", "-r",
        default="eu-west-1",
        help="AWS region (default: eu-west-1)"
    )
    
    args = parser.parse_args()
    
    logger.info(f"Uploading files from {args.source} to s3://{args.bucket}/{args.prefix}")
    
    stats = upload_directory(
        source_dir=args.source,
        bucket=args.bucket,
        prefix=args.prefix,
        region=args.region
    )
    
    logger.info("=" * 50)
    logger.info(f"Upload complete!")
    logger.info(f"  ✅ Uploaded: {stats['uploaded']}")
    logger.info(f"  ❌ Failed: {stats['failed']}")
    logger.info(f"  ⏭️  Skipped: {stats['skipped']}")
    
    if stats["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
