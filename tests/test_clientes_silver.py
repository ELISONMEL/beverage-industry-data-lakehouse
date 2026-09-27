from pyspark.sql.types import StringType, StructField, StructType

from src.transformation.clientes_silver import (
    apply_quality_rules,
    deduplicate_clientes,
    split_valid_invalid,
    transform_clientes,
)

SCHEMA = StructType([
    StructField("cliente_id", StringType(), True),
    StructField("nome_cliente", StringType(), True),
    StructField("tipo_cliente", StringType(), True),
    StructField("documento", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("data_cadastro", StringType(), True),
    StructField("status_cliente", StringType(), True),
])


def test_padronizacao_cliente(spark):

    df = spark.createDataFrame(
        [
            (
                "1",
                " EMPRESA TESTE ",
                " supermercado ",
                "12.345.678/0001-00",
                " MANAUS ",
                "am",
                "2026-01-10",
                " ativo "
            )
        ],
        schema=SCHEMA
    )

    result = transform_clientes(df).collect()[0]

    assert result.cliente_id == 1
    assert result.cidade == "Manaus"
    assert result.estado == "AM"
    assert result.status_cliente == "ATIVO"
    assert result.tipo_cliente == "SUPERMERCADO"


def test_cliente_id_obrigatorio(spark):

    df = spark.createDataFrame(
        [
            (
                None,
                "Cliente Teste",
                "SUPERMERCADO",
                "12.345.678/0001-00",
                "Manaus",
                "AM",
                "2026-01-10",
                "ATIVO"
            )
        ],
        schema=SCHEMA
    )

    transformed = transform_clientes(df)

    result = (
        apply_quality_rules(transformed)
        .collect()[0]
    )

    assert result["_dq_reason"] == "CLIENTE_ID_NULL"


def test_estado_invalido(spark):

    df = spark.createDataFrame(
        [
            (
                "10",
                "Cliente Teste",
                "EMPRESA",
                "12.345.678/0001-00",
                "Manaus",
                "Amazonas",
                "2026-01-10",
                "ATIVO"
            )
        ],
        schema=SCHEMA
    )

    transformed = transform_clientes(df)

    result = (
        apply_quality_rules(transformed)
        .collect()[0]
    )

    assert result["_dq_reason"] == "ESTADO_INVALIDO"


def test_status_nulo_invalido(spark):

    df = spark.createDataFrame(
        [
            (
                "11",
                "Cliente Teste",
                "EMPRESA",
                "12.345.678/0001-00",
                "Manaus",
                "AM",
                "2026-01-10",
                None
            )
        ],
        schema=SCHEMA
    )

    transformed = transform_clientes(df)

    result = (
        apply_quality_rules(transformed)
        .collect()[0]
    )

    assert result["_dq_reason"] == "STATUS_INVALIDO"

def test_deduplicacao_cliente(spark):
    df = spark.createDataFrame(
        [
            (
                "10",
                "Cliente Teste",
                "EMPRESA",
                "12.345.678/0001-00",
                "Manaus",
                "AM",
                "2026-01-10",
                "ATIVO",
            ),
            (
                "10",
                "Cliente Teste",
                "EMPRESA",
                "12.345.678/0001-00",
                "Manaus",
                "AM",
                "2026-01-10",
                "ATIVO",
            ),
        ],
        schema=SCHEMA,
    )

    transformed = transform_clientes(df)
    result = deduplicate_clientes(transformed)

    assert result.count() == 1
    assert result.collect()[0]["cliente_id"] == 10


def test_separacao_validos_invalidos(spark):
    df = spark.createDataFrame(
        [
            (
                "10",
                "Cliente Valido",
                "EMPRESA",
                "12.345.678/0001-00",
                "Manaus",
                "AM",
                "2026-01-10",
                "ATIVO",
            ),
            (
                "20",
                "Cliente Invalido",
                "EMPRESA",
                "98.765.432/0001-00",
                "Manaus",
                "Amazonas",
                "2026-01-10",
                "ATIVO",
            ),
        ],
        schema=SCHEMA,
    )

    transformed = transform_clientes(df)
    quality = apply_quality_rules(transformed)

    valid, invalid = split_valid_invalid(quality)

    assert valid.count() == 1
    assert invalid.count() == 1

    assert valid.collect()[0]["cliente_id"] == 10

    invalid_row = invalid.collect()[0]

    assert invalid_row["cliente_id"] == 20
    assert invalid_row["_dq_reason"] == "ESTADO_INVALIDO"

    assert "_dq_reason" not in valid.columns
    assert "_dq_reason" in invalid.columns