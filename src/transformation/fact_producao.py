from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format


def create_fact_producao(
    producao_df: DataFrame,
    dim_produto_df: DataFrame,
) -> DataFrame:
    """
    Cria a fato de produção da camada Gold.

    Grão:
    uma linha por registro de produção/lote.
    """

    producao_base = (
        producao_df
        .select(
            "producao_id",
            "produto_id",
            "data_producao",
            "lote",
            "turno",
            "quantidade_produzida",
            "quantidade_descartada",
            "linha_producao",
        )
        .withColumn(
            "data_sk",
            date_format(
                col("data_producao"),
                "yyyyMMdd",
            ).cast("long"),
        )
        .withColumn(
            "quantidade_aprovada",
            col("quantidade_produzida")
            - col("quantidade_descartada"),
        )
    )

    produto_keys = dim_produto_df.select(
        "produto_id",
        "produto_sk",
    )

    fact_df = (
        producao_base
        .join(
            produto_keys,
            "produto_id",
            "inner",
        )
    )

    return fact_df.select(
        col("producao_id").cast("long"),
        col("produto_sk").cast("long"),
        col("data_sk").cast("long"),
        "lote",
        "turno",
        "linha_producao",
        col("quantidade_produzida").cast("long"),
        col("quantidade_descartada").cast("long"),
        col("quantidade_aprovada").cast("long"),
    )