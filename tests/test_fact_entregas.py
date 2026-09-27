from datetime import date

from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.transformation.fact_entregas import create_fact_entregas

DISTRIBUICAO_SCHEMA = StructType(
    [
        StructField("entrega_id", LongType(), False),
        StructField("pedido_id", LongType(), False),
        StructField("data_saida", DateType(), False),
        StructField("data_prevista", DateType(), False),
        StructField("data_entrega", DateType(), False),
        StructField("transportadora", StringType(), True),
        StructField("rota", StringType(), True),
    ]
)


def test_criacao_fact_entregas(spark):
    df = spark.createDataFrame(
        [
            (
                100,
                1000,
                date(2026, 9, 20),
                date(2026, 9, 25),
                date(2026, 9, 26),
                "TRANSPORTADORA A",
                "ROTA-01",
            )
        ],
        schema=DISTRIBUICAO_SCHEMA,
    )

    result = create_fact_entregas(df).collect()[0]

    assert result["entrega_id"] == 100
    assert result["pedido_id"] == 1000

    assert result["data_saida_sk"] == 20260920
    assert result["data_prevista_sk"] == 20260925
    assert result["data_entrega_sk"] == 20260926

    assert result["transportadora"] == "TRANSPORTADORA A"
    assert result["rota"] == "ROTA-01"

    assert result["dias_transporte"] == 6
    assert result["dias_atraso"] == 1
    assert result["status_entrega"] == "ATRASADA"


def test_fact_entregas_no_prazo(spark):
    df = spark.createDataFrame(
        [
            (
                100,
                1000,
                date(2026, 9, 20),
                date(2026, 9, 25),
                date(2026, 9, 25),
                "TRANSPORTADORA A",
                "ROTA-01",
            )
        ],
        schema=DISTRIBUICAO_SCHEMA,
    )

    result = create_fact_entregas(df).collect()[0]

    assert result["dias_transporte"] == 5
    assert result["dias_atraso"] == 0
    assert result["status_entrega"] == "NO_PRAZO"


def test_fact_entregas_antecipada(spark):
    df = spark.createDataFrame(
        [
            (
                100,
                1000,
                date(2026, 9, 20),
                date(2026, 9, 25),
                date(2026, 9, 23),
                "TRANSPORTADORA A",
                "ROTA-01",
            )
        ],
        schema=DISTRIBUICAO_SCHEMA,
    )

    result = create_fact_entregas(df).collect()[0]

    assert result["dias_transporte"] == 3
    assert result["dias_atraso"] == 0
    assert result["status_entrega"] == "NO_PRAZO"


def test_fact_entregas_calcula_dias_atraso(spark):
    df = spark.createDataFrame(
        [
            (
                100,
                1000,
                date(2026, 9, 20),
                date(2026, 9, 24),
                date(2026, 9, 28),
                "TRANSPORTADORA A",
                "ROTA-01",
            )
        ],
        schema=DISTRIBUICAO_SCHEMA,
    )

    result = create_fact_entregas(df).collect()[0]

    assert result["dias_transporte"] == 8
    assert result["dias_atraso"] == 4
    assert result["status_entrega"] == "ATRASADA"


def test_fact_entregas_preserva_grao(spark):
    df = spark.createDataFrame(
        [
            (
                100,
                1000,
                date(2026, 9, 20),
                date(2026, 9, 25),
                date(2026, 9, 25),
                "TRANSPORTADORA A",
                "ROTA-01",
            ),
            (
                101,
                1001,
                date(2026, 9, 21),
                date(2026, 9, 26),
                date(2026, 9, 27),
                "TRANSPORTADORA B",
                "ROTA-02",
            ),
        ],
        schema=DISTRIBUICAO_SCHEMA,
    )

    result = create_fact_entregas(df)

    assert result.count() == 2

    ids = {
        row["entrega_id"]
        for row in result.select("entrega_id").collect()
    }

    assert ids == {100, 101}


def test_fact_entregas_colunas(spark):
    df = spark.createDataFrame(
        [
            (
                100,
                1000,
                date(2026, 9, 20),
                date(2026, 9, 25),
                date(2026, 9, 25),
                "TRANSPORTADORA A",
                "ROTA-01",
            )
        ],
        schema=DISTRIBUICAO_SCHEMA,
    )

    result = create_fact_entregas(df)

    assert result.columns == [
        "entrega_id",
        "pedido_id",
        "data_saida_sk",
        "data_prevista_sk",
        "data_entrega_sk",
        "transportadora",
        "rota",
        "dias_transporte",
        "dias_atraso",
        "status_entrega",
    ]


def test_fact_entregas_tipos(spark):
    df = spark.createDataFrame(
        [
            (
                100,
                1000,
                date(2026, 9, 20),
                date(2026, 9, 25),
                date(2026, 9, 25),
                "TRANSPORTADORA A",
                "ROTA-01",
            )
        ],
        schema=DISTRIBUICAO_SCHEMA,
    )

    result = create_fact_entregas(df)

    schema = {
        field.name: field.dataType
        for field in result.schema.fields
    }

    assert isinstance(schema["entrega_id"], LongType)
    assert isinstance(schema["pedido_id"], LongType)

    assert isinstance(schema["data_saida_sk"], LongType)
    assert isinstance(schema["data_prevista_sk"], LongType)
    assert isinstance(schema["data_entrega_sk"], LongType)