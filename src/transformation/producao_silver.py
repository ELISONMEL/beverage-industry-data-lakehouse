from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    to_date,
    trim,
    upper,
    when,
)


def transform_producao(df: DataFrame) -> DataFrame:

    return (
        df
        .withColumn(
            "producao_id",
            col("producao_id").cast("long"),
        )
        .withColumn(
            "produto_id",
            col("produto_id").cast("long"),
        )
        .withColumn(
            "data_producao",
            to_date(col("data_producao")),
        )
        .withColumn(
            "lote",
            upper(trim(col("lote"))),
        )
        .withColumn(
            "turno",
            upper(trim(col("turno"))),
        )
        .withColumn(
            "quantidade_produzida",
            col("quantidade_produzida").cast("long"),
        )
        .withColumn(
            "quantidade_descartada",
            col("quantidade_descartada").cast("long"),
        )
        .withColumn(
            "linha_producao",
            upper(trim(col("linha_producao"))),
        )
    )


def deduplicate_producao(
    df: DataFrame
) -> DataFrame:

    return df.dropDuplicates(
        ["producao_id"]
    )


def apply_producao_quality(
    df: DataFrame
) -> DataFrame:

    turnos_validos = [
        "MANHA",
        "TARDE",
        "NOITE",
    ]

    return df.withColumn(
        "_dq_reason",

        when(
            col("producao_id").isNull(),
            lit("PRODUCAO_ID_NULL"),
        )

        .when(
            col("produto_id").isNull(),
            lit("PRODUTO_ID_NULL"),
        )

        .when(
            col("data_producao").isNull(),
            lit("DATA_PRODUCAO_INVALIDA"),
        )

        .when(
            col("lote").isNull()
            | (trim(col("lote")) == ""),
            lit("LOTE_NULL"),
        )

        .when(
            col("turno").isNull()
            | (
                ~col("turno")
                .isin(turnos_validos)
            ),
            lit("TURNO_INVALIDO"),
        )

        .when(
            col("quantidade_produzida").isNull(),
            lit("QUANTIDADE_PRODUZIDA_NULL"),
        )

        .when(
            col("quantidade_produzida") <= 0,
            lit("QUANTIDADE_PRODUZIDA_INVALIDA"),
        )

        .when(
            col("quantidade_descartada").isNull(),
            lit("QUANTIDADE_DESCARTADA_NULL"),
        )

        .when(
            col("quantidade_descartada") < 0,
            lit("QUANTIDADE_DESCARTADA_INVALIDA"),
        )

        .when(
            col("quantidade_descartada")
            > col("quantidade_produzida"),
            lit("DESCARTE_MAIOR_QUE_PRODUCAO"),
        )

        .when(
            col("linha_producao").isNull()
            | (trim(col("linha_producao")) == ""),
            lit("LINHA_PRODUCAO_NULL"),
        )
    )