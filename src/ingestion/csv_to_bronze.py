import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_date,
    current_timestamp,
    input_file_name,
    lit,
)

from src.audit.processed_files import (
    calculate_file_hash,
    register_processed_file,
    was_file_processed,
)


def ingest_csv_to_bronze(
    spark: SparkSession,
    source_path: str,
    destination_path: str,
    audit_path: str,
    schema,
):
    batch_id = str(uuid.uuid4())

    file_hash = calculate_file_hash(source_path)

    if was_file_processed(
        spark=spark,
        audit_path=audit_path,
        file_hash=file_hash,
    ):
        return {
            "batch_id": batch_id,
            "records": 0,
            "status": "SKIPPED",
            "message": "Arquivo já processado",
        }

    df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(source_path)
    )

    df_bronze = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_load_date", current_date())
        .withColumn("_source_file", input_file_name())
        .withColumn("_batch_id", lit(batch_id))
    )

    records = df_bronze.count()

    (
        df_bronze.write
        .format("delta")
        .mode("append")
        .save(destination_path)
    )

    register_processed_file(
        spark=spark,
        audit_path=audit_path,
        source_file=source_path,
        file_hash=file_hash,
        batch_id=batch_id,
        status="SUCCESS",
    )

    return {
        "batch_id": batch_id,
        "records": records,
        "status": "SUCCESS",
    }
