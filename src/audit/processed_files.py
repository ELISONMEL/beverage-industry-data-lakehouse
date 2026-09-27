import hashlib
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


def calculate_file_hash(file_path: str) -> str:
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as file:
        for chunk in iter(lambda: file.read(8192), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def was_file_processed(
    spark: SparkSession,
    audit_path: str,
    file_hash: str
) -> bool:

    if not Path(audit_path).exists():
        return False

    df = (
        spark.read
        .format("delta")
        .load(audit_path)
    )

    return (
        df
        .filter(df.file_hash == file_hash)
        .limit(1)
        .count()
        > 0
    )


def register_processed_file(
    spark: SparkSession,
    audit_path: str,
    source_file: str,
    file_hash: str,
    batch_id: str,
    status: str = "SUCCESS"
):

    data = [
        (
            source_file,
            file_hash,
            batch_id,
            status
        )
    ]

    columns = [
        "source_file",
        "file_hash",
        "batch_id",
        "status"
    ]

    df = (
        spark.createDataFrame(data, columns)
        .withColumn(
            "processed_at",
            current_timestamp()
        )
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .save(audit_path)
    )