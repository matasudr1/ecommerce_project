"""Utility functions package."""

from .spark_utils import (
    get_spark_session,
    read_csv_with_schema,
    write_parquet_partitioned,
    add_metadata_columns,
    deduplicate_by_key,
    cast_columns,
    null_safe_trim,
    validate_not_null,
)

from .s3_utils import (
    get_s3_client,
    list_s3_objects,
    build_s3_path,
    upload_file_to_s3,
    check_path_exists,
)

__all__ = [
    # Spark utilities
    "get_spark_session",
    "read_csv_with_schema",
    "write_parquet_partitioned",
    "add_metadata_columns",
    "deduplicate_by_key",
    "cast_columns",
    "null_safe_trim",
    "validate_not_null",
    # S3 utilities
    "get_s3_client",
    "list_s3_objects",
    "build_s3_path",
    "upload_file_to_s3",
    "check_path_exists",
]
