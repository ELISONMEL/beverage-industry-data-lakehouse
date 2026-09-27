from pyspark.sql.types import StringType, StructField, StructType

from src.transformation.producao_silver import (
    apply_producao_quality,
    deduplicate_producao,
    transform_producao,
)

COLUMNS = [
    "producao_id",
    "produto_id",
    "data_producao",
    "lote",
    "turno",
    "quantidade_produzida",
    "quantidade_descartada",
    "linha_producao",
]

INPUT_SCHEMA = StructType(
    [
        StructField(column, StringType(), True)
        for column in COLUMNS
    ]
)


def create_producao_df(
    spark,
    producao_id="100",
    produto_id="10",
    data_producao="2026-01-15",
    lote="LOTE-001",
    turno="MANHA",
    quantidade_produzida="1000",
    quantidade_descartada="20",
    linha_producao="LINHA-01",
):
    return spark.createDataFrame(
        [
            (
                producao_id,
                produto_id,
                data_producao,
                lote,
                turno,
                quantidade_produzida,
                quantidade_descartada,
                linha_producao,
            )
        ],
        INPUT_SCHEMA,
    )


def get_dq_reason(spark, **kwargs):
    df = create_producao_df(spark, **kwargs)
    transformed = transform_producao(df)
    result = apply_producao_quality(transformed).collect()[0]

    return result["_dq_reason"]


def test_transformacao_producao(spark):
    df = create_producao_df(
        spark,
        lote=" lote-001 ",
        turno=" manha ",
        linha_producao=" linha-01 ",
    )

    result = transform_producao(df).collect()[0]

    assert result["producao_id"] == 100
    assert result["produto_id"] == 10
    assert result["data_producao"].isoformat() == "2026-01-15"
    assert result["lote"] == "LOTE-001"
    assert result["turno"] == "MANHA"
    assert result["quantidade_produzida"] == 1000
    assert result["quantidade_descartada"] == 20
    assert result["linha_producao"] == "LINHA-01"


def test_producao_valida(spark):
    assert get_dq_reason(spark) is None


def test_producao_id_nulo(spark):
    assert get_dq_reason(
        spark,
        producao_id=None,
    ) == "PRODUCAO_ID_NULL"


def test_produto_id_nulo(spark):
    assert get_dq_reason(
        spark,
        produto_id=None,
    ) == "PRODUTO_ID_NULL"


def test_data_producao_invalida(spark):
    assert get_dq_reason(
        spark,
        data_producao="data-invalida",
    ) == "DATA_PRODUCAO_INVALIDA"


def test_lote_nulo(spark):
    assert get_dq_reason(
        spark,
        lote=None,
    ) == "LOTE_NULL"


def test_turno_invalido(spark):
    assert get_dq_reason(
        spark,
        turno="MADRUGADA",
    ) == "TURNO_INVALIDO"


def test_quantidade_produzida_invalida(spark):
    assert get_dq_reason(
        spark,
        quantidade_produzida="0",
    ) == "QUANTIDADE_PRODUZIDA_INVALIDA"


def test_quantidade_descartada_invalida(spark):
    assert get_dq_reason(
        spark,
        quantidade_descartada="-1",
    ) == "QUANTIDADE_DESCARTADA_INVALIDA"


def test_descarte_maior_que_producao(spark):
    assert get_dq_reason(
        spark,
        quantidade_produzida="100",
        quantidade_descartada="101",
    ) == "DESCARTE_MAIOR_QUE_PRODUCAO"


def test_linha_producao_nula(spark):
    assert get_dq_reason(
        spark,
        linha_producao=None,
    ) == "LINHA_PRODUCAO_NULL"


def test_deduplicacao_producao(spark):
    df = spark.createDataFrame(
        [
            (
                "100",
                "10",
                "2026-01-15",
                "LOTE-001",
                "MANHA",
                "1000",
                "20",
                "LINHA-01",
            ),
            (
                "100",
                "10",
                "2026-01-15",
                "LOTE-001",
                "MANHA",
                "1000",
                "20",
                "LINHA-01",
            ),
        ],
        INPUT_SCHEMA,
    )

    transformed = transform_producao(df)
    result = deduplicate_producao(transformed)

    assert result.count() == 1
    assert result.collect()[0]["producao_id"] == 100