from datetime import date

from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.transformation.dim_cliente import create_dim_cliente

SCHEMA = StructType(
    [
        StructField("cliente_id", LongType(), False),
        StructField("nome_cliente", StringType(), True),
        StructField("tipo_cliente", StringType(), True),
        StructField("documento", StringType(), True),
        StructField("cidade", StringType(), True),
        StructField("estado", StringType(), True),
        StructField("data_cadastro", DateType(), True),
        StructField("status_cliente", StringType(), True),
    ]
)


def test_criacao_dim_cliente(spark):
    df = spark.createDataFrame(
        [
            (
                20,
                "Cliente B",
                "EMPRESA",
                "22.222.222/0001-22",
                "Sao Paulo",
                "SP",
                date(2025, 2, 10),
                "ATIVO",
            ),
            (
                10,
                "Cliente A",
                "SUPERMERCADO",
                "11.111.111/0001-11",
                "Manaus",
                "AM",
                date(2025, 1, 10),
                "ATIVO",
            ),
        ],
        schema=SCHEMA,
    )

    result = (
        create_dim_cliente(df)
        .orderBy("cliente_sk")
        .collect()
    )

    assert len(result) == 2

    assert result[0]["cliente_sk"] == 1
    assert result[0]["cliente_id"] == 10

    assert result[1]["cliente_sk"] == 2
    assert result[1]["cliente_id"] == 20


def test_dim_cliente_remove_duplicados(spark):
    df = spark.createDataFrame(
        [
            (
                10,
                "Cliente A",
                "SUPERMERCADO",
                "11.111.111/0001-11",
                "Manaus",
                "AM",
                date(2025, 1, 10),
                "ATIVO",
            ),
            (
                10,
                "Cliente A",
                "SUPERMERCADO",
                "11.111.111/0001-11",
                "Manaus",
                "AM",
                date(2025, 1, 10),
                "ATIVO",
            ),
        ],
        schema=SCHEMA,
    )

    result = create_dim_cliente(df)

    assert result.count() == 1

    row = result.collect()[0]

    assert row["cliente_id"] == 10
    assert row["cliente_sk"] == 1


def test_dim_cliente_aplica_minimizacao_de_dados(spark):
    df = spark.createDataFrame(
        [
            (
                10,
                "Cliente A",
                "SUPERMERCADO",
                "11.111.111/0001-11",
                "Manaus",
                "AM",
                date(2025, 1, 10),
                "ATIVO",
            )
        ],
        schema=SCHEMA,
    )

    result = create_dim_cliente(df)

    assert "documento" not in result.columns

    assert result.columns == [
        "cliente_sk",
        "cliente_id",
        "nome_cliente",
        "tipo_cliente",
        "cidade",
        "estado",
        "data_cadastro",
        "status_cliente",
    ]


def test_dim_cliente_tipos_das_chaves(spark):
    df = spark.createDataFrame(
        [
            (
                10,
                "Cliente A",
                "SUPERMERCADO",
                "11.111.111/0001-11",
                "Manaus",
                "AM",
                date(2025, 1, 10),
                "ATIVO",
            )
        ],
        schema=SCHEMA,
    )

    result = create_dim_cliente(df)

    schema = {
        field.name: field.dataType
        for field in result.schema.fields
    }

    assert isinstance(schema["cliente_sk"], LongType)
    assert isinstance(schema["cliente_id"], LongType)