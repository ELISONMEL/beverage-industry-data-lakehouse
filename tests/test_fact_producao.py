from datetime import date

from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.transformation.fact_producao import create_fact_producao

PRODUCAO_SCHEMA = StructType(
    [
        StructField("producao_id", LongType(), False),
        StructField("produto_id", LongType(), False),
        StructField("data_producao", DateType(), False),
        StructField("lote", StringType(), True),
        StructField("turno", StringType(), True),
        StructField("quantidade_produzida", LongType(), False),
        StructField("quantidade_descartada", LongType(), False),
        StructField("linha_producao", StringType(), True),
    ]
)

DIM_PRODUTO_SCHEMA = StructType(
    [
        StructField("produto_id", LongType(), False),
        StructField("produto_sk", LongType(), False),
    ]
)


def criar_dados_base(spark):
    producao = spark.createDataFrame(
        [
            (
                100,
                10,
                date(2026, 9, 26),
                "LOTE-001",
                "MANHA",
                1000,
                50,
                "LINHA-01",
            )
        ],
        schema=PRODUCAO_SCHEMA,
    )

    dim_produto = spark.createDataFrame(
        [(10, 1000)],
        schema=DIM_PRODUTO_SCHEMA,
    )

    return producao, dim_produto


def test_criacao_fact_producao(spark):
    producao, dim_produto = criar_dados_base(spark)

    result = create_fact_producao(
        producao,
        dim_produto,
    ).collect()[0]

    assert result["producao_id"] == 100
    assert result["produto_sk"] == 1000
    assert result["data_sk"] == 20260926

    assert result["lote"] == "LOTE-001"
    assert result["turno"] == "MANHA"
    assert result["linha_producao"] == "LINHA-01"

    assert result["quantidade_produzida"] == 1000
    assert result["quantidade_descartada"] == 50
    assert result["quantidade_aprovada"] == 950


def test_fact_producao_calcula_quantidade_aprovada(spark):
    producao, dim_produto = criar_dados_base(spark)

    result = create_fact_producao(
        producao,
        dim_produto,
    ).collect()[0]

    assert (
        result["quantidade_aprovada"]
        == result["quantidade_produzida"]
        - result["quantidade_descartada"]
    )


def test_fact_producao_preserva_grao(spark):
    producao = spark.createDataFrame(
        [
            (
                100,
                10,
                date(2026, 9, 26),
                "LOTE-001",
                "MANHA",
                1000,
                50,
                "LINHA-01",
            ),
            (
                101,
                10,
                date(2026, 9, 26),
                "LOTE-002",
                "TARDE",
                800,
                20,
                "LINHA-02",
            ),
        ],
        schema=PRODUCAO_SCHEMA,
    )

    dim_produto = spark.createDataFrame(
        [(10, 1000)],
        schema=DIM_PRODUTO_SCHEMA,
    )

    result = create_fact_producao(
        producao,
        dim_produto,
    )

    assert result.count() == 2

    ids = {
        row["producao_id"]
        for row in result.select("producao_id").collect()
    }

    assert ids == {100, 101}


def test_fact_producao_remove_produto_sem_dimensao(spark):
    producao, _ = criar_dados_base(spark)

    dim_produto = spark.createDataFrame(
        [(999, 9999)],
        schema=DIM_PRODUTO_SCHEMA,
    )

    result = create_fact_producao(
        producao,
        dim_produto,
    )

    assert result.count() == 0


def test_fact_producao_colunas(spark):
    producao, dim_produto = criar_dados_base(spark)

    result = create_fact_producao(
        producao,
        dim_produto,
    )

    assert result.columns == [
        "producao_id",
        "produto_sk",
        "data_sk",
        "lote",
        "turno",
        "linha_producao",
        "quantidade_produzida",
        "quantidade_descartada",
        "quantidade_aprovada",
    ]


def test_fact_producao_tipos(spark):
    producao, dim_produto = criar_dados_base(spark)

    result = create_fact_producao(
        producao,
        dim_produto,
    )

    schema = {
        field.name: field.dataType
        for field in result.schema.fields
    }

    assert isinstance(schema["producao_id"], LongType)
    assert isinstance(schema["produto_sk"], LongType)
    assert isinstance(schema["data_sk"], LongType)

    assert isinstance(schema["quantidade_produzida"], LongType)
    assert isinstance(schema["quantidade_descartada"], LongType)
    assert isinstance(schema["quantidade_aprovada"], LongType)