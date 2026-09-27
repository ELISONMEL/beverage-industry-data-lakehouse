from pyspark.sql.types import StringType, StructField, StructType

from src.transformation.produtos_silver import (
    apply_produtos_quality,
    deduplicate_produtos,
    split_valid_invalid,
    transform_produtos,
)

COLUMNS = [
    "produto_id",
    "descricao",
    "categoria",
    "volume_ml",
    "tipo_embalagem",
    "preco_unitario",
    "custo_unitario",
    "status_produto",
]

INPUT_SCHEMA = StructType(
    [
        StructField(column, StringType(), True)
        for column in COLUMNS
    ]
)


def test_transformacao_produto(spark):
    df = spark.createDataFrame(
        [
            (
                "10",
                "  agua mineral 500ml  ",
                " agua_mineral ",
                "500",
                " pet ",
                "5.50",
                "3.20",
                " ativo ",
            )
        ],
        COLUMNS,
    )

    result = transform_produtos(df).collect()[0]

    assert result["produto_id"] == 10
    assert result["descricao"] == "Agua Mineral 500ml"
    assert result["categoria"] == "AGUA_MINERAL"
    assert result["volume_ml"] == 500
    assert result["tipo_embalagem"] == "PET"
    assert result["preco_unitario"] == 5.50
    assert result["custo_unitario"] == 3.20
    assert result["status_produto"] == "ATIVO"


def create_produto_df(
    spark,
    produto_id="10",
    descricao="Agua Mineral 500ml",
    volume_ml="500",
    preco="5.50",
    custo="3.20",
    status="ATIVO",
):
    return spark.createDataFrame(
        [
            (
                produto_id,
                descricao,
                "AGUA_MINERAL",
                volume_ml,
                "PET",
                preco,
                custo,
                status,
            )
        ],
        INPUT_SCHEMA,
    )


def get_dq_reason(spark, **kwargs):
    df = create_produto_df(spark, **kwargs)
    transformed = transform_produtos(df)
    result = apply_produtos_quality(transformed).collect()[0]

    return result["_dq_reason"]


def test_produto_valido(spark):
    assert get_dq_reason(spark) is None


def test_produto_id_nulo(spark):
    assert get_dq_reason(
        spark,
        produto_id=None,
    ) == "PRODUTO_ID_NULL"


def test_volume_invalido(spark):
    assert get_dq_reason(
        spark,
        volume_ml="0",
    ) == "VOLUME_INVALIDO"


def test_preco_invalido(spark):
    assert get_dq_reason(
        spark,
        preco="-1.00",
    ) == "PRECO_INVALIDO"


def test_custo_maior_que_preco(spark):
    assert get_dq_reason(
        spark,
        preco="5.00",
        custo="6.00",
    ) == "CUSTO_MAIOR_QUE_PRECO"


def test_status_invalido(spark):
    assert get_dq_reason(
        spark,
        status="DESCONHECIDO",
    ) == "STATUS_INVALIDO"

def test_deduplicacao_produto(spark):
    df = spark.createDataFrame(
        [
            (
                "10",
                "Agua Mineral 500ml",
                "AGUA_MINERAL",
                "500",
                "PET",
                "5.50",
                "3.20",
                "ATIVO",
            ),
            (
                "10",
                "Agua Mineral 500ml",
                "AGUA_MINERAL",
                "500",
                "PET",
                "5.50",
                "3.20",
                "ATIVO",
            ),
        ],
        INPUT_SCHEMA,
    )

    transformed = transform_produtos(df)
    result = deduplicate_produtos(transformed)

    assert result.count() == 1
    assert result.collect()[0]["produto_id"] == 10


def test_separacao_validos_invalidos(spark):
    df = spark.createDataFrame(
        [
            (
                "10",
                "Agua Mineral 500ml",
                "AGUA_MINERAL",
                "500",
                "PET",
                "5.50",
                "3.20",
                "ATIVO",
            ),
            (
                "20",
                "Agua Mineral 1L",
                "AGUA_MINERAL",
                "1000",
                "PET",
                "-1.00",
                "0.50",
                "ATIVO",
            ),
        ],
        INPUT_SCHEMA,
    )

    transformed = transform_produtos(df)
    quality = apply_produtos_quality(transformed)

    valid, invalid = split_valid_invalid(quality)

    assert valid.count() == 1
    assert invalid.count() == 1

    assert valid.collect()[0]["produto_id"] == 10

    invalid_row = invalid.collect()[0]
    assert invalid_row["produto_id"] == 20
    assert invalid_row["_dq_reason"] == "PRECO_INVALIDO"

    assert "_dq_reason" not in valid.columns
    assert "_dq_reason" in invalid.columns