from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    initcap,
    lit,
    to_date,
    trim,
    upper,
    when,
)


def transform_vendedores(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn(
            "vendedor_id",
            col("vendedor_id").cast("long"),
        )
        .withColumn(
            "nome_vendedor",
            initcap(trim(col("nome_vendedor"))),
        )
        .withColumn(
            "cidade_base",
            initcap(trim(col("cidade_base"))),
        )
        .withColumn(
            "estado",
            upper(trim(col("estado"))),
        )
        .withColumn(
            "data_admissao",
            to_date(col("data_admissao")),
        )
        .withColumn(
            "status_vendedor",
            upper(trim(col("status_vendedor"))),
        )
    )


def deduplicate_vendedores(
    df: DataFrame
) -> DataFrame:

    return df.dropDuplicates(
        ["vendedor_id"]
    )


def apply_vendedores_quality(
    df: DataFrame
) -> DataFrame:

    status_validos = [
        "ATIVO",
        "INATIVO",
    ]

    return df.withColumn(
        "_dq_reason",

        when(
            col("vendedor_id").isNull(),
            lit("VENDEDOR_ID_NULL"),
        )

        .when(
            col("nome_vendedor").isNull()
            | (trim(col("nome_vendedor")) == ""),
            lit("NOME_VENDEDOR_NULL"),
        )

        .when(
            col("estado").isNull(),
            lit("ESTADO_NULL"),
        )

        .when(
            ~col("estado").rlike("^[A-Z]{2}$"),
            lit("ESTADO_INVALIDO"),
        )

        .when(
            col("data_admissao").isNull(),
            lit("DATA_ADMISSAO_INVALIDA"),
        )

        .when(
            col("status_vendedor").isNull()
            | (
                ~col("status_vendedor")
                .isin(status_validos)
            ),
            lit("STATUS_INVALIDO"),
        )
    )


def split_valid_invalid(
    df: DataFrame
):

    valid = (
        df
        .filter(
            col("_dq_reason").isNull()
        )
        .drop("_dq_reason")
    )

    invalid = (
        df
        .filter(
            col("_dq_reason").isNotNull()
        )
    )

    return valid, invalid