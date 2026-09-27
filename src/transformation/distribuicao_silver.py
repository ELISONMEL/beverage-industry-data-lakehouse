from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    to_date,
    trim,
    upper,
    when,
)


def transform_distribuicao(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn(
            "entrega_id",
            col("entrega_id").cast("long"),
        )
        .withColumn(
            "pedido_id",
            col("pedido_id").cast("long"),
        )
        .withColumn(
            "data_saida",
            to_date(col("data_saida")),
        )
        .withColumn(
            "data_prevista",
            to_date(col("data_prevista")),
        )
        .withColumn(
            "data_entrega",
            to_date(col("data_entrega")),
        )
        .withColumn(
            "transportadora",
            upper(trim(col("transportadora"))),
        )
        .withColumn(
            "rota",
            upper(trim(col("rota"))),
        )
    )


def deduplicate_distribuicao(
    df: DataFrame
) -> DataFrame:

    return df.dropDuplicates(
        ["entrega_id"]
    )


def apply_distribuicao_quality(
    df: DataFrame
) -> DataFrame:

    return df.withColumn(
        "_dq_reason",

        when(
            col("entrega_id").isNull(),
            lit("ENTREGA_ID_NULL"),
        )

        .when(
            col("pedido_id").isNull(),
            lit("PEDIDO_ID_NULL"),
        )

        .when(
            col("data_saida").isNull(),
            lit("DATA_SAIDA_INVALIDA"),
        )

        .when(
            col("data_prevista").isNull(),
            lit("DATA_PREVISTA_INVALIDA"),
        )

        .when(
            col("data_entrega").isNull(),
            lit("DATA_ENTREGA_INVALIDA"),
        )

        .when(
            col("data_entrega")
            < col("data_saida"),
            lit("ENTREGA_ANTES_DA_SAIDA"),
        )

        .when(
            col("transportadora").isNull()
            | (trim(col("transportadora")) == ""),
            lit("TRANSPORTADORA_NULL"),
        )

        .when(
            col("rota").isNull()
            | (trim(col("rota")) == ""),
            lit("ROTA_NULL"),
        )
    )