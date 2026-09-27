from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    to_date,
    trim,
    upper,
    when,
)


def transform_estoque(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn(
            "estoque_id",
            col("estoque_id").cast("long"),
        )
        .withColumn(
            "produto_id",
            col("produto_id").cast("long"),
        )
        .withColumn(
            "data_referencia",
            to_date(col("data_referencia")),
        )
        .withColumn(
            "quantidade_estoque",
            col("quantidade_estoque").cast("long"),
        )
        .withColumn(
            "estoque_minimo",
            col("estoque_minimo").cast("long"),
        )
        .withColumn(
            "estoque_maximo",
            col("estoque_maximo").cast("long"),
        )
        .withColumn(
            "local_estoque",
            upper(trim(col("local_estoque"))),
        )
    )


def deduplicate_estoque(
    df: DataFrame
) -> DataFrame:

    return df.dropDuplicates(
        ["estoque_id"]
    )


def apply_estoque_quality(
    df: DataFrame
) -> DataFrame:

    return df.withColumn(
        "_dq_reason",

        when(
            col("estoque_id").isNull(),
            lit("ESTOQUE_ID_NULL"),
        )

        .when(
            col("produto_id").isNull(),
            lit("PRODUTO_ID_NULL"),
        )

        .when(
            col("data_referencia").isNull(),
            lit("DATA_REFERENCIA_INVALIDA"),
        )

        .when(
            col("quantidade_estoque").isNull(),
            lit("QUANTIDADE_ESTOQUE_NULL"),
        )

        .when(
            col("quantidade_estoque") < 0,
            lit("QUANTIDADE_ESTOQUE_INVALIDA"),
        )

        .when(
            col("estoque_minimo").isNull(),
            lit("ESTOQUE_MINIMO_NULL"),
        )

        .when(
            col("estoque_minimo") < 0,
            lit("ESTOQUE_MINIMO_INVALIDO"),
        )

        .when(
            col("estoque_maximo").isNull(),
            lit("ESTOQUE_MAXIMO_NULL"),
        )

        .when(
            col("estoque_maximo") <= 0,
            lit("ESTOQUE_MAXIMO_INVALIDO"),
        )

        .when(
            col("estoque_minimo")
            > col("estoque_maximo"),
            lit("MINIMO_MAIOR_QUE_MAXIMO"),
        )

        .when(
            col("local_estoque").isNull()
            | (trim(col("local_estoque")) == ""),
            lit("LOCAL_ESTOQUE_NULL"),
        )
    )