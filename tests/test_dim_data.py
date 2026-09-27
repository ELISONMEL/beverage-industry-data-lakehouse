from datetime import date

from pyspark.sql.types import DateType, StructField, StructType

from src.transformation.dim_data import create_dim_data

SCHEMA = StructType(
    [
        StructField("data", DateType(), False),
    ]
)


def test_criacao_dim_data(spark):
    df = spark.createDataFrame(
        [
            (date(2026, 9, 23),),
        ],
        schema=SCHEMA,
    )

    result = create_dim_data(df).collect()[0]

    assert result["data_sk"] == 20260923
    assert result["data"] == date(2026, 9, 23)
    assert result["ano"] == 2026
    assert result["mes"] == 9
    assert result["nome_mes"] == "SETEMBRO"
    assert result["trimestre"] == 3
    assert result["dia"] == 23
    assert result["dia_semana"] == 4
    assert result["nome_dia_semana"] == "QUARTA"
    assert result["fim_de_semana"] is False


def test_dim_data_identifica_sabado(spark):
    df = spark.createDataFrame(
        [
            (date(2026, 9, 26),),
        ],
        schema=SCHEMA,
    )

    result = create_dim_data(df).collect()[0]

    assert result["dia_semana"] == 7
    assert result["nome_dia_semana"] == "SABADO"
    assert result["fim_de_semana"] is True


def test_dim_data_identifica_domingo(spark):
    df = spark.createDataFrame(
        [
            (date(2026, 9, 27),),
        ],
        schema=SCHEMA,
    )

    result = create_dim_data(df).collect()[0]

    assert result["dia_semana"] == 1
    assert result["nome_dia_semana"] == "DOMINGO"
    assert result["fim_de_semana"] is True


def test_dim_data_remove_duplicados(spark):
    df = spark.createDataFrame(
        [
            (date(2026, 9, 26),),
            (date(2026, 9, 26),),
        ],
        schema=SCHEMA,
    )

    result = create_dim_data(df)

    assert result.count() == 1
    assert result.collect()[0]["data_sk"] == 20260926


def test_dim_data_colunas(spark):
    df = spark.createDataFrame(
        [
            (date(2026, 9, 26),),
        ],
        schema=SCHEMA,
    )

    result = create_dim_data(df)

    assert result.columns == [
        "data_sk",
        "data",
        "ano",
        "mes",
        "nome_mes",
        "trimestre",
        "dia",
        "dia_semana",
        "nome_dia_semana",
        "fim_de_semana",
    ]