from pyspark.sql.types import StringType, StructField, StructType

from src.transformation.estoque_silver import (
    apply_estoque_quality,
    deduplicate_estoque,
    transform_estoque,
)

COLUMNS = [
    "estoque_id",
    "produto_id",
    "data_referencia",
    "quantidade_estoque",
    "estoque_minimo",
    "estoque_maximo",
    "local_estoque",
]

INPUT_SCHEMA = StructType(
    [
        StructField(column, StringType(), True)
        for column in COLUMNS
    ]
)


def create_estoque_df(
    spark,
    estoque_id="100",
    produto_id="10",
    data_referencia="2026-01-15",
    quantidade_estoque="500",
    estoque_minimo="100",
    estoque_maximo="1000",
    local_estoque="CD MANAUS",
):
    return spark.createDataFrame(
        [
            (
                estoque_id,
                produto_id,
                data_referencia,
                quantidade_estoque,
                estoque_minimo,
                estoque_maximo,
                local_estoque,
            )
        ],
        INPUT_SCHEMA,
    )


def get_dq_reason(spark, **kwargs):
    df = create_estoque_df(spark, **kwargs)
    transformed = transform_estoque(df)
    result = apply_estoque_quality(transformed).collect()[0]

    return result["_dq_reason"]


def test_transformacao_estoque(spark):
    df = create_estoque_df(
        spark,
        local_estoque=" cd manaus ",
    )

    result = transform_estoque(df).collect()[0]

    assert result["estoque_id"] == 100
    assert result["produto_id"] == 10
    assert result["data_referencia"].isoformat() == "2026-01-15"
    assert result["quantidade_estoque"] == 500
    assert result["estoque_minimo"] == 100
    assert result["estoque_maximo"] == 1000
    assert result["local_estoque"] == "CD MANAUS"


def test_estoque_valido(spark):
    assert get_dq_reason(spark) is None


def test_estoque_id_nulo(spark):
    assert get_dq_reason(
        spark,
        estoque_id=None,
    ) == "ESTOQUE_ID_NULL"


def test_produto_id_nulo(spark):
    assert get_dq_reason(
        spark,
        produto_id=None,
    ) == "PRODUTO_ID_NULL"


def test_data_referencia_invalida(spark):
    assert get_dq_reason(
        spark,
        data_referencia="data-invalida",
    ) == "DATA_REFERENCIA_INVALIDA"


def test_quantidade_estoque_invalida(spark):
    assert get_dq_reason(
        spark,
        quantidade_estoque="-1",
    ) == "QUANTIDADE_ESTOQUE_INVALIDA"


def test_estoque_minimo_invalido(spark):
    assert get_dq_reason(
        spark,
        estoque_minimo="-1",
    ) == "ESTOQUE_MINIMO_INVALIDO"


def test_estoque_maximo_invalido(spark):
    assert get_dq_reason(
        spark,
        estoque_maximo="0",
    ) == "ESTOQUE_MAXIMO_INVALIDO"


def test_minimo_maior_que_maximo(spark):
    assert get_dq_reason(
        spark,
        estoque_minimo="1001",
        estoque_maximo="1000",
    ) == "MINIMO_MAIOR_QUE_MAXIMO"


def test_local_estoque_nulo(spark):
    assert get_dq_reason(
        spark,
        local_estoque=None,
    ) == "LOCAL_ESTOQUE_NULL"


def test_deduplicacao_estoque(spark):
    df = spark.createDataFrame(
        [
            (
                "100",
                "10",
                "2026-01-15",
                "500",
                "100",
                "1000",
                "CD MANAUS",
            ),
            (
                "100",
                "10",
                "2026-01-15",
                "500",
                "100",
                "1000",
                "CD MANAUS",
            ),
        ],
        INPUT_SCHEMA,
    )

    transformed = transform_estoque(df)
    result = deduplicate_estoque(transformed)

    assert result.count() == 1
    assert result.collect()[0]["estoque_id"] == 100