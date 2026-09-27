from datetime import date

from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.transformation.dim_vendedor import create_dim_vendedor

SCHEMA = StructType(
    [
        StructField("vendedor_id", LongType(), False),
        StructField("nome_vendedor", StringType(), True),
        StructField("cidade_base", StringType(), True),
        StructField("estado", StringType(), True),
        StructField("data_admissao", DateType(), True),
        StructField("status_vendedor", StringType(), True),
    ]
)


def test_criacao_dim_vendedor(spark):
    df = spark.createDataFrame(
        [
            (
                20,
                "Vendedor B",
                "Sao Paulo",
                "SP",
                date(2020, 2, 10),
                "ATIVO",
            ),
            (
                10,
                "Vendedor A",
                "Manaus",
                "AM",
                date(2019, 1, 10),
                "ATIVO",
            ),
        ],
        schema=SCHEMA,
    )

    result = (
        create_dim_vendedor(df)
        .orderBy("vendedor_sk")
        .collect()
    )

    assert len(result) == 2

    assert result[0]["vendedor_sk"] == 1
    assert result[0]["vendedor_id"] == 10

    assert result[1]["vendedor_sk"] == 2
    assert result[1]["vendedor_id"] == 20


def test_dim_vendedor_remove_duplicados(spark):
    df = spark.createDataFrame(
        [
            (
                10,
                "Vendedor A",
                "Manaus",
                "AM",
                date(2019, 1, 10),
                "ATIVO",
            ),
            (
                10,
                "Vendedor A",
                "Manaus",
                "AM",
                date(2019, 1, 10),
                "ATIVO",
            ),
        ],
        schema=SCHEMA,
    )

    result = create_dim_vendedor(df)

    assert result.count() == 1

    row = result.collect()[0]

    assert row["vendedor_id"] == 10
    assert row["vendedor_sk"] == 1


def test_dim_vendedor_colunas(spark):
    df = spark.createDataFrame(
        [
            (
                10,
                "Vendedor A",
                "Manaus",
                "AM",
                date(2019, 1, 10),
                "ATIVO",
            )
        ],
        schema=SCHEMA,
    )

    result = create_dim_vendedor(df)

    assert result.columns == [
        "vendedor_sk",
        "vendedor_id",
        "nome_vendedor",
        "cidade_base",
        "estado",
        "data_admissao",
        "status_vendedor",
    ]


def test_dim_vendedor_tipos_das_chaves(spark):
    df = spark.createDataFrame(
        [
            (
                10,
                "Vendedor A",
                "Manaus",
                "AM",
                date(2019, 1, 10),
                "ATIVO",
            )
        ],
        schema=SCHEMA,
    )

    result = create_dim_vendedor(df)

    schema = {
        field.name: field.dataType
        for field in result.schema.fields
    }

    assert isinstance(schema["vendedor_sk"], LongType)
    assert isinstance(schema["vendedor_id"], LongType)