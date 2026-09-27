from datetime import date

from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.transformation.fact_estoque import create_fact_estoque

ESTOQUE_SCHEMA = StructType(
    [
        StructField("estoque_id", LongType(), False),
        StructField("produto_id", LongType(), False),
        StructField("data_referencia", DateType(), False),
        StructField("quantidade_estoque", LongType(), False),
        StructField("estoque_minimo", LongType(), False),
        StructField("estoque_maximo", LongType(), False),
        StructField("local_estoque", StringType(), True),
    ]
)

DIM_PRODUTO_SCHEMA = StructType(
    [
        StructField("produto_id", LongType(), False),
        StructField("produto_sk", LongType(), False),
    ]
)


def criar_dados_base(spark):
    estoque = spark.createDataFrame(
        [
            (
                100,
                10,
                date(2026, 9, 26),
                500,
                100,
                1000,
                "CD-MANAUS",
            )
        ],
        schema=ESTOQUE_SCHEMA,
    )

    dim_produto = spark.createDataFrame(
        [(10, 1000)],
        schema=DIM_PRODUTO_SCHEMA,
    )

    return estoque, dim_produto


def test_criacao_fact_estoque(spark):
    estoque, dim_produto = criar_dados_base(spark)

    result = create_fact_estoque(
        estoque,
        dim_produto,
    ).collect()[0]

    assert result["estoque_id"] == 100
    assert result["produto_sk"] == 1000
    assert result["data_sk"] == 20260926
    assert result["local_estoque"] == "CD-MANAUS"

    assert result["quantidade_estoque"] == 500
    assert result["estoque_minimo"] == 100
    assert result["estoque_maximo"] == 1000
    assert result["saldo_vs_minimo"] == 400
    assert result["status_estoque"] == "NORMAL"


def test_fact_estoque_classifica_status(spark):
    estoque = spark.createDataFrame(
        [
            (1, 10, date(2026, 9, 26), 50, 100, 1000, "CD-01"),
            (2, 10, date(2026, 9, 26), 500, 100, 1000, "CD-01"),
            (3, 10, date(2026, 9, 26), 1200, 100, 1000, "CD-01"),
        ],
        schema=ESTOQUE_SCHEMA,
    )

    dim_produto = spark.createDataFrame(
        [(10, 1000)],
        schema=DIM_PRODUTO_SCHEMA,
    )

    result = {
        row["estoque_id"]: row["status_estoque"]
        for row in create_fact_estoque(
            estoque,
            dim_produto,
        ).collect()
    }

    assert result[1] == "ABAIXO_MINIMO"
    assert result[2] == "NORMAL"
    assert result[3] == "ACIMA_MAXIMO"


def test_fact_estoque_limites_sao_normais(spark):
    estoque = spark.createDataFrame(
        [
            (1, 10, date(2026, 9, 26), 100, 100, 1000, "CD-01"),
            (2, 10, date(2026, 9, 26), 1000, 100, 1000, "CD-01"),
        ],
        schema=ESTOQUE_SCHEMA,
    )

    dim_produto = spark.createDataFrame(
        [(10, 1000)],
        schema=DIM_PRODUTO_SCHEMA,
    )

    result = create_fact_estoque(
        estoque,
        dim_produto,
    ).collect()

    assert all(
        row["status_estoque"] == "NORMAL"
        for row in result
    )


def test_fact_estoque_calcula_saldo_vs_minimo(spark):
    estoque, dim_produto = criar_dados_base(spark)

    result = create_fact_estoque(
        estoque,
        dim_produto,
    ).collect()[0]

    assert (
        result["saldo_vs_minimo"]
        == result["quantidade_estoque"]
        - result["estoque_minimo"]
    )


def test_fact_estoque_remove_produto_sem_dimensao(spark):
    estoque, _ = criar_dados_base(spark)

    dim_produto = spark.createDataFrame(
        [(999, 9999)],
        schema=DIM_PRODUTO_SCHEMA,
    )

    result = create_fact_estoque(
        estoque,
        dim_produto,
    )

    assert result.count() == 0


def test_fact_estoque_colunas(spark):
    estoque, dim_produto = criar_dados_base(spark)

    result = create_fact_estoque(
        estoque,
        dim_produto,
    )

    assert result.columns == [
        "estoque_id",
        "produto_sk",
        "data_sk",
        "local_estoque",
        "quantidade_estoque",
        "estoque_minimo",
        "estoque_maximo",
        "saldo_vs_minimo",
        "status_estoque",
    ]


def test_fact_estoque_tipos(spark):
    estoque, dim_produto = criar_dados_base(spark)

    result = create_fact_estoque(
        estoque,
        dim_produto,
    )

    schema = {
        field.name: field.dataType
        for field in result.schema.fields
    }

    assert isinstance(schema["estoque_id"], LongType)
    assert isinstance(schema["produto_sk"], LongType)
    assert isinstance(schema["data_sk"], LongType)

    assert isinstance(schema["quantidade_estoque"], LongType)
    assert isinstance(schema["estoque_minimo"], LongType)
    assert isinstance(schema["estoque_maximo"], LongType)
    assert isinstance(schema["saldo_vs_minimo"], LongType)