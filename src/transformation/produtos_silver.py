from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    initcap,
    lit,
    trim,
    upper,
    when,
)


def transform_produtos(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn(
            "produto_id",
            col("produto_id").cast("long"),
        )
        .withColumn(
            "descricao",
            initcap(trim(col("descricao"))),
        )
        .withColumn(
            "categoria",
            upper(trim(col("categoria"))),
        )
        .withColumn(
            "volume_ml",
            col("volume_ml").cast("integer"),
        )
        .withColumn(
            "tipo_embalagem",
            upper(trim(col("tipo_embalagem"))),
        )
        .withColumn(
            "preco_unitario",
            col("preco_unitario").cast("double"),
        )
        .withColumn(
            "custo_unitario",
            col("custo_unitario").cast("double"),
        )
        .withColumn(
            "status_produto",
            upper(trim(col("status_produto"))),
        )
    )


def deduplicate_produtos(df: DataFrame) -> DataFrame:

    return df.dropDuplicates(
        ["produto_id"]
    )


def apply_produtos_quality(
    df: DataFrame
) -> DataFrame:

    status_validos = [
        "ATIVO",
        "INATIVO",
    ]

    return df.withColumn(
        "_dq_reason",

        when(
            col("produto_id").isNull(),
            lit("PRODUTO_ID_NULL"),
        )

        .when(
            col("descricao").isNull()
            | (trim(col("descricao")) == ""),
            lit("DESCRICAO_NULL"),
        )

        .when(
            col("volume_ml").isNull(),
            lit("VOLUME_NULL"),
        )

        .when(
            col("volume_ml") <= 0,
            lit("VOLUME_INVALIDO"),
        )

        .when(
            col("preco_unitario").isNull(),
            lit("PRECO_NULL"),
        )

        .when(
            col("preco_unitario") <= 0,
            lit("PRECO_INVALIDO"),
        )

        .when(
            col("custo_unitario").isNull(),
            lit("CUSTO_NULL"),
        )

        .when(
            col("custo_unitario") <= 0,
            lit("CUSTO_INVALIDO"),
        )

        .when(
            col("custo_unitario")
            > col("preco_unitario"),
            lit("CUSTO_MAIOR_QUE_PRECO"),
        )

        .when(
            col("status_produto").isNull()
            | (
                ~col("status_produto")
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