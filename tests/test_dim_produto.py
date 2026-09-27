from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.transformation.dim_produto import create_dim_produto

SCHEMA = StructType(
    [
        StructField("produto_id", LongType(), False),
        StructField("descricao", StringType(), True),
        StructField("categoria", StringType(), True),
        StructField("volume_ml", LongType(), True),
        StructField("tipo_embalagem", StringType(), True),
        StructField("preco_unitario", DoubleType(), True),
        StructField("custo_unitario", DoubleType(), True),
        StructField("status_produto", StringType(), True),
    ]
)


def test_criacao_dim_produto(spark):
    df = spark.createDataFrame(
        [
            (
                20,
                "Refrigerante 2L",
                "REFRIGERANTE",
                2000,
                "PET",
                10.50,
                6.00,
                "ATIVO",
            ),
            (
                10,
                "Agua Mineral 500ml",
                "AGUA_MINERAL",
                500,
                "PET",
                5.50,
                3.20,
                "ATIVO",
            ),
        ],
        schema=SCHEMA,
    )

    result = (
        create_dim_produto(df)
        .orderBy("produto_sk")
        .collect()
    )

    assert len(result) == 2

    assert result[0]["produto_sk"] == 1
    assert result[0]["produto_id"] == 10

    assert result[1]["produto_sk"] == 2
    assert result[1]["produto_id"] == 20


def test_dim_produto_remove_duplicados(spark):
    df = spark.createDataFrame(
        [
            (
                10,
                "Agua Mineral 500ml",
                "AGUA_MINERAL",
                500,
                "PET",
                5.50,
                3.20,
                "ATIVO",
            ),
            (
                10,
                "Agua Mineral 500ml",
                "AGUA_MINERAL",
                500,
                "PET",
                5.50,
                3.20,
                "ATIVO",
            ),
        ],
        schema=SCHEMA,
    )

    result = create_dim_produto(df)

    assert result.count() == 1

    row = result.collect()[0]

    assert row["produto_id"] == 10
    assert row["produto_sk"] == 1


def test_dim_produto_colunas(spark):
    df = spark.createDataFrame(
        [
            (
                10,
                "Agua Mineral 500ml",
                "AGUA_MINERAL",
                500,
                "PET",
                5.50,
                3.20,
                "ATIVO",
            )
        ],
        schema=SCHEMA,
    )

    result = create_dim_produto(df)

    assert result.columns == [
        "produto_sk",
        "produto_id",
        "descricao",
        "categoria",
        "volume_ml",
        "tipo_embalagem",
        "preco_unitario",
        "custo_unitario",
        "status_produto",
    ]


def test_dim_produto_tipos_das_chaves(spark):
    df = spark.createDataFrame(
        [
            (
                10,
                "Agua Mineral 500ml",
                "AGUA_MINERAL",
                500,
                "PET",
                5.50,
                3.20,
                "ATIVO",
            )
        ],
        schema=SCHEMA,
    )

    result = create_dim_produto(df)

    schema = {
        field.name: field.dataType
        for field in result.schema.fields
    }

    assert isinstance(schema["produto_sk"], LongType)
    assert isinstance(schema["produto_id"], LongType)