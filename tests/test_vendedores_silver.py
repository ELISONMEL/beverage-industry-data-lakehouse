from pyspark.sql.types import StringType, StructField, StructType

from src.transformation.vendedores_silver import (
    apply_vendedores_quality,
    deduplicate_vendedores,
    split_valid_invalid,
    transform_vendedores,
)

COLUMNS = [
    "vendedor_id",
    "nome_vendedor",
    "cidade_base",
    "estado",
    "data_admissao",
    "status_vendedor",
]

INPUT_SCHEMA = StructType(
    [
        StructField(column, StringType(), True)
        for column in COLUMNS
    ]
)


def create_vendedor_df(
    spark,
    vendedor_id="10",
    nome="joao da silva",
    cidade="manaus",
    estado="AM",
    data_admissao="2020-05-10",
    status="ATIVO",
):
    return spark.createDataFrame(
        [
            (
                vendedor_id,
                nome,
                cidade,
                estado,
                data_admissao,
                status,
            )
        ],
        INPUT_SCHEMA,
    )


def get_dq_reason(spark, **kwargs):
    df = create_vendedor_df(spark, **kwargs)
    transformed = transform_vendedores(df)
    result = apply_vendedores_quality(transformed).collect()[0]

    return result["_dq_reason"]


def test_transformacao_vendedor(spark):
    df = create_vendedor_df(
        spark,
        nome="  joao da silva  ",
        cidade="  manaus  ",
        estado=" am ",
        status=" ativo ",
    )

    result = transform_vendedores(df).collect()[0]

    assert result["vendedor_id"] == 10
    assert result["nome_vendedor"] == "Joao Da Silva"
    assert result["cidade_base"] == "Manaus"
    assert result["estado"] == "AM"
    assert result["data_admissao"].isoformat() == "2020-05-10"
    assert result["status_vendedor"] == "ATIVO"


def test_vendedor_valido(spark):
    assert get_dq_reason(spark) is None


def test_vendedor_id_nulo(spark):
    assert get_dq_reason(
        spark,
        vendedor_id=None,
    ) == "VENDEDOR_ID_NULL"


def test_nome_vendedor_nulo(spark):
    assert get_dq_reason(
        spark,
        nome=None,
    ) == "NOME_VENDEDOR_NULL"


def test_estado_invalido(spark):
    assert get_dq_reason(
        spark,
        estado="Amazonas",
    ) == "ESTADO_INVALIDO"


def test_data_admissao_invalida(spark):
    assert get_dq_reason(
        spark,
        data_admissao="data-invalida",
    ) == "DATA_ADMISSAO_INVALIDA"


def test_status_invalido(spark):
    assert get_dq_reason(
        spark,
        status="BLOQUEADO",
    ) == "STATUS_INVALIDO"


def test_deduplicacao_vendedor(spark):
    df = spark.createDataFrame(
        [
            (
                "10",
                "Joao da Silva",
                "Manaus",
                "AM",
                "2020-05-10",
                "ATIVO",
            ),
            (
                "10",
                "Joao da Silva",
                "Manaus",
                "AM",
                "2020-05-10",
                "ATIVO",
            ),
        ],
        INPUT_SCHEMA,
    )

    transformed = transform_vendedores(df)
    result = deduplicate_vendedores(transformed)

    assert result.count() == 1
    assert result.collect()[0]["vendedor_id"] == 10


def test_separacao_validos_invalidos(spark):
    df = spark.createDataFrame(
        [
            (
                "10",
                "Joao da Silva",
                "Manaus",
                "AM",
                "2020-05-10",
                "ATIVO",
            ),
            (
                "20",
                "Maria Souza",
                "Sao Paulo",
                "INVALIDO",
                "2021-03-15",
                "ATIVO",
            ),
        ],
        INPUT_SCHEMA,
    )

    transformed = transform_vendedores(df)
    quality = apply_vendedores_quality(transformed)

    valid, invalid = split_valid_invalid(quality)

    assert valid.count() == 1
    assert invalid.count() == 1

    assert valid.collect()[0]["vendedor_id"] == 10

    invalid_row = invalid.collect()[0]

    assert invalid_row["vendedor_id"] == 20
    assert invalid_row["_dq_reason"] == "ESTADO_INVALIDO"

    assert "_dq_reason" not in valid.columns
    assert "_dq_reason" in invalid.columns