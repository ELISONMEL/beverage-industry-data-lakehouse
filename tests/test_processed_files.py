from unittest.mock import MagicMock, patch

from src.audit.processed_files import (
    calculate_file_hash,
    was_file_processed,
)


def test_calculate_file_hash(tmp_path):
    file_path = tmp_path / "arquivo_teste.csv"
    file_path.write_text(
        "cliente_id,nome\n1,Cliente A\n",
        encoding="utf-8",
    )

    result = calculate_file_hash(str(file_path))

    assert result == (
        "9b24bb6fc2e556c849cc7fea9b834866"
        "48b7172eea42eb5bfa33b61ab1f7d686"
    )

def test_was_file_processed_retorna_false_quando_auditoria_nao_existe(
    spark,
    tmp_path,
):
    audit_path = tmp_path / "audit" / "processed_files"

    result = was_file_processed(
        spark=spark,
        audit_path=str(audit_path),
        file_hash="hash-inexistente",
    )

    assert result is False

def test_register_processed_file_grava_registro_delta():
    from src.audit.processed_files import register_processed_file

    spark = MagicMock()

    df_created = MagicMock()
    df_with_timestamp = MagicMock()

    spark.createDataFrame.return_value = df_created
    df_created.withColumn.return_value = df_with_timestamp

    with patch("src.audit.processed_files.current_timestamp") as mock_timestamp:
        register_processed_file(
            spark=spark,
            audit_path="/tmp/audit",
            source_file="clientes.csv",
            file_hash="abc123",
            batch_id="batch-001",
        )

    spark.createDataFrame.assert_called_once_with(
        [
            (
                "clientes.csv",
                "abc123",
                "batch-001",
                "SUCCESS",
            )
        ],
        [
            "source_file",
            "file_hash",
            "batch_id",
            "status",
        ],
    )

    df_created.withColumn.assert_called_once_with(
        "processed_at",
        mock_timestamp.return_value,
    )

    df_with_timestamp.write.format.assert_called_once_with("delta")
    df_with_timestamp.write.format.return_value.mode.assert_called_once_with(
        "append"
    )
    (
        df_with_timestamp.write
        .format.return_value
        .mode.return_value
        .save.assert_called_once_with("/tmp/audit")
    )

def test_was_file_processed_retorna_true_quando_hash_existe(tmp_path):
    audit_path = tmp_path / "audit"
    audit_path.mkdir()

    spark = MagicMock()
    df = MagicMock()

    spark.read.format.return_value.load.return_value = df

    df.file_hash.__eq__.return_value = "filtro-hash"
    df.filter.return_value.limit.return_value.count.return_value = 1

    result = was_file_processed(
        spark=spark,
        audit_path=str(audit_path),
        file_hash="abc123",
    )

    assert result is True

    spark.read.format.assert_called_once_with("delta")
    spark.read.format.return_value.load.assert_called_once_with(
        str(audit_path)
    )
    df.filter.assert_called_once_with("filtro-hash")
    df.filter.return_value.limit.assert_called_once_with(1)


def test_was_file_processed_retorna_false_quando_hash_nao_existe(tmp_path):
    audit_path = tmp_path / "audit"
    audit_path.mkdir()

    spark = MagicMock()
    df = MagicMock()

    spark.read.format.return_value.load.return_value = df

    df.file_hash.__eq__.return_value = "filtro-hash"
    df.filter.return_value.limit.return_value.count.return_value = 0

    result = was_file_processed(
        spark=spark,
        audit_path=str(audit_path),
        file_hash="hash-inexistente",
    )

    assert result is False