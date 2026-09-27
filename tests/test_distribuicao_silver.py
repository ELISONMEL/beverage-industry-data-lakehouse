from pyspark.sql.types import StringType, StructField, StructType

from src.transformation.distribuicao_silver import (
    apply_distribuicao_quality,
    deduplicate_distribuicao,
    transform_distribuicao,
)

COLUMNS = [
    "entrega_id",
    "pedido_id",
    "data_saida",
    "data_prevista",
    "data_entrega",
    "transportadora",
    "rota",
]

INPUT_SCHEMA = StructType(
    [
        StructField(column, StringType(), True)
        for column in COLUMNS
    ]
)


def create_distribuicao_df(
    spark,
    entrega_id="100",
    pedido_id="1000",
    data_saida="2026-01-10",
    data_prevista="2026-01-15",
    data_entrega="2026-01-14",
    transportadora="TRANSPORTADORA A",
    rota="ROTA NORTE",
):
    return spark.createDataFrame(
        [
            (
                entrega_id,
                pedido_id,
                data_saida,
                data_prevista,
                data_entrega,
                transportadora,
                rota,
            )
        ],
        INPUT_SCHEMA,
    )


def get_dq_reason(spark, **kwargs):
    df = create_distribuicao_df(spark, **kwargs)
    transformed = transform_distribuicao(df)
    result = apply_distribuicao_quality(transformed).collect()[0]

    return result["_dq_reason"]


def test_transformacao_distribuicao(spark):
    df = create_distribuicao_df(
        spark,
        transportadora=" transportadora a ",
        rota=" rota norte ",
    )

    result = transform_distribuicao(df).collect()[0]

    assert result["entrega_id"] == 100
    assert result["pedido_id"] == 1000
    assert result["data_saida"].isoformat() == "2026-01-10"
    assert result["data_prevista"].isoformat() == "2026-01-15"
    assert result["data_entrega"].isoformat() == "2026-01-14"
    assert result["transportadora"] == "TRANSPORTADORA A"
    assert result["rota"] == "ROTA NORTE"


def test_distribuicao_valida(spark):
    assert get_dq_reason(spark) is None


def test_entrega_id_nulo(spark):
    assert get_dq_reason(
        spark,
        entrega_id=None,
    ) == "ENTREGA_ID_NULL"


def test_pedido_id_nulo(spark):
    assert get_dq_reason(
        spark,
        pedido_id=None,
    ) == "PEDIDO_ID_NULL"


def test_data_saida_invalida(spark):
    assert get_dq_reason(
        spark,
        data_saida="data-invalida",
    ) == "DATA_SAIDA_INVALIDA"


def test_data_prevista_invalida(spark):
    assert get_dq_reason(
        spark,
        data_prevista="data-invalida",
    ) == "DATA_PREVISTA_INVALIDA"


def test_data_entrega_invalida(spark):
    assert get_dq_reason(
        spark,
        data_entrega="data-invalida",
    ) == "DATA_ENTREGA_INVALIDA"


def test_entrega_antes_da_saida(spark):
    assert get_dq_reason(
        spark,
        data_saida="2026-01-10",
        data_entrega="2026-01-09",
    ) == "ENTREGA_ANTES_DA_SAIDA"


def test_entrega_atrasada_continua_valida(spark):
    assert get_dq_reason(
        spark,
        data_saida="2026-01-10",
        data_prevista="2026-01-15",
        data_entrega="2026-01-20",
    ) is None


def test_transportadora_nula(spark):
    assert get_dq_reason(
        spark,
        transportadora=None,
    ) == "TRANSPORTADORA_NULL"


def test_rota_nula(spark):
    assert get_dq_reason(
        spark,
        rota=None,
    ) == "ROTA_NULL"


def test_deduplicacao_distribuicao(spark):
    df = spark.createDataFrame(
        [
            (
                "100",
                "1000",
                "2026-01-10",
                "2026-01-15",
                "2026-01-14",
                "TRANSPORTADORA A",
                "ROTA NORTE",
            ),
            (
                "100",
                "1000",
                "2026-01-10",
                "2026-01-15",
                "2026-01-14",
                "TRANSPORTADORA A",
                "ROTA NORTE",
            ),
        ],
        INPUT_SCHEMA,
    )

    transformed = transform_distribuicao(df)
    result = deduplicate_distribuicao(transformed)

    assert result.count() == 1
    assert result.collect()[0]["entrega_id"] == 100