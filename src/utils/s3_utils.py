"""Utility functions for S3 operations."""

import boto3
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


def get_s3_client(region: str = "eu-west-1"):
    """
    Get an S3 client.
    
    In AWS Glue, credentials are automatically provided via IAM roles.
    For local development, uses credentials from ~/.aws/credentials
    
    Args:
        region: AWS region
        
    Returns:
        boto3 S3 client
    """
    return boto3.client('s3', region_name=region)


def list_s3_objects(
    bucket: str,
    prefix: str,
    suffix: Optional[str] = None
) -> List[str]:
    """
    List objects in an S3 bucket with optional filtering.
    
    Args:
        bucket: S3 bucket name
        prefix: Prefix to filter objects
        suffix: Optional suffix filter (e.g., '.csv')
        
    Returns:
        List of S3 keys
    """
    s3 = get_s3_client()
    
    objects = []
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if suffix is None or key.endswith(suffix):
                    objects.append(key)
    
    return objects


def build_s3_path(bucket: str, *parts: str) -> str:
    """
    Build an S3 path from parts.
    
    Args:
        bucket: S3 bucket name
        *parts: Path components
        
    Returns:
        Full S3 path (s3://bucket/path/to/object)
    """
    path = "/".join(p.strip("/") for p in parts if p)
    return f"s3://{bucket}/{path}"


def upload_file_to_s3(
    local_path: str,
    bucket: str,
    key: str
) -> None:
    """
    Upload a local file to S3.
    
    Args:
        local_path: Path to local file
        bucket: Destination bucket
        key: Destination key (path within bucket)
    """
    s3 = get_s3_client()
    s3.upload_file(local_path, bucket, key)
    logger.info(f"Uploaded {local_path} to s3://{bucket}/{key}")


def check_path_exists(bucket: str, prefix: str) -> bool:
    """
    Check if an S3 path exists (has any objects).
    
    Args:
        bucket: S3 bucket name
        prefix: Prefix to check
        
    Returns:
        True if any objects exist with the prefix
    """
    s3 = get_s3_client()
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return 'Contents' in response
