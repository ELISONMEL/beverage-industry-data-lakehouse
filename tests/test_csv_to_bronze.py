from unittest.mock import MagicMock, patch

from src.ingestion.csv_to_bronze import ingest_csv_to_bronze


@patch("src.ingestion.csv_to_bronze.was_file_processed")
@patch("src.ingestion.csv_to_bronze.calculate_file_hash")
def test_ingest_csv_to_bronze_skips_arquivo_ja_processado(
    mock_calculate_hash,
    mock_was_processed,
):
    spark = MagicMock()

    mock_calculate_hash.return_value = "hash-123"
    mock_was_processed.return_value = True

    result = ingest_csv_to_bronze(
        spark=spark,
        source_path="/tmp/clientes.csv",
        destination_path="/tmp/bronze/clientes",
        audit_path="/tmp/audit",
        schema=MagicMock(),
    )

    assert result["records"] == 0
    assert result["status"] == "SKIPPED"
    assert result["message"] == "Arquivo já processado"
    assert result["batch_id"]

    mock_calculate_hash.assert_called_once_with(
        "/tmp/clientes.csv"
    )

    mock_was_processed.assert_called_once_with(
        spark=spark,
        audit_path="/tmp/audit",
        file_hash="hash-123",
    )

    spark.read.option.assert_not_called()


@patch("src.ingestion.csv_to_bronze.lit")
@patch("src.ingestion.csv_to_bronze.input_file_name")
@patch("src.ingestion.csv_to_bronze.current_date")
@patch("src.ingestion.csv_to_bronze.current_timestamp")
@patch("src.ingestion.csv_to_bronze.register_processed_file")
@patch("src.ingestion.csv_to_bronze.was_file_processed")
@patch("src.ingestion.csv_to_bronze.calculate_file_hash")
def test_ingest_csv_to_bronze_processa_arquivo_novo(
    mock_calculate_hash,
    mock_was_processed,
    mock_register,
    mock_current_timestamp,
    mock_current_date,
    mock_input_file_name,
    mock_lit,
):
    spark = MagicMock()
    schema = MagicMock()

    mock_calculate_hash.return_value = "hash-456"
    mock_was_processed.return_value = False

    df = MagicMock()
    df_bronze = MagicMock()

    (
        spark.read
        .option.return_value
        .option.return_value
        .schema.return_value
        .csv.return_value
    ) = df

    df.withColumn.return_value = df_bronze
    df_bronze.withColumn.return_value = df_bronze
    df_bronze.count.return_value = 10

    result = ingest_csv_to_bronze(
        spark=spark,
        source_path="/tmp/clientes.csv",
        destination_path="/tmp/bronze/clientes",
        audit_path="/tmp/audit",
        schema=schema,
    )

    # Validação dos metadados da camada Bronze

    df.withColumn.assert_called_once_with(
        "_ingestion_timestamp",
        mock_current_timestamp.return_value,
    )

    assert df_bronze.withColumn.call_count == 3

    df_bronze.withColumn.assert_any_call(
        "_load_date",
        mock_current_date.return_value,
    )

    df_bronze.withColumn.assert_any_call(
        "_source_file",
        mock_input_file_name.return_value,
    )

    mock_lit.assert_called_once_with(result["batch_id"])

    df_bronze.withColumn.assert_any_call(
        "_batch_id",
        mock_lit.return_value,
    )

    # Validação do resultado da ingestão

    assert result["records"] == 10
    assert result["status"] == "SUCCESS"
    assert result["batch_id"]

    # Validação do hash e da idempotência

    mock_calculate_hash.assert_called_once_with(
        "/tmp/clientes.csv"
    )

    mock_was_processed.assert_called_once_with(
        spark=spark,
        audit_path="/tmp/audit",
        file_hash="hash-456",
    )

    # Validação da leitura do CSV

    spark.read.option.assert_called_once_with(
        "header",
        True,
    )

    (
        spark.read
        .option.return_value
        .option.assert_called_once_with(
            "mode",
            "PERMISSIVE",
        )
    )

    (
        spark.read
        .option.return_value
        .option.return_value
        .schema.assert_called_once_with(schema)
    )

    (
        spark.read
        .option.return_value
        .option.return_value
        .schema.return_value
        .csv.assert_called_once_with("/tmp/clientes.csv")
    )

    # Validação da contagem

    df_bronze.count.assert_called_once_with()

    # Validação da escrita Delta

    df_bronze.write.format.assert_called_once_with("delta")

    (
        df_bronze.write
        .format.return_value
        .mode.assert_called_once_with("append")
    )

    (
        df_bronze.write
        .format.return_value
        .mode.return_value
        .save.assert_called_once_with("/tmp/bronze/clientes")
    )

    # Validação do registro na auditoria

    mock_register.assert_called_once_with(
        spark=spark,
        audit_path="/tmp/audit",
        source_file="/tmp/clientes.csv",
        file_hash="hash-456",
        batch_id=result["batch_id"],
        status="SUCCESS",
    )