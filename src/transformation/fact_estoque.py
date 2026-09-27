from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, lit, when


def create_fact_estoque(
    estoque_df: DataFrame,
    dim_produto_df: DataFrame,
) -> DataFrame:
    """
    Cria a fato de estoque da camada Gold.

    Grão:
    uma linha por registro/snapshot de estoque.
    """

    estoque_base = (
        estoque_df
        .select(
            "estoque_id",
            "produto_id",
            "data_referencia",
            "quantidade_estoque",
            "estoque_minimo",
            "estoque_maximo",
            "local_estoque",
        )
        .withColumn(
            "data_sk",
            date_format(
                col("data_referencia"),
                "yyyyMMdd",
            ).cast("long"),
        )
        .withColumn(
            "saldo_vs_minimo",
            col("quantidade_estoque") - col("estoque_minimo"),
        )
        .withColumn(
            "status_estoque",
            when(
                col("quantidade_estoque") < col("estoque_minimo"),
                lit("ABAIXO_MINIMO"),
            )
            .when(
                col("quantidade_estoque") > col("estoque_maximo"),
                lit("ACIMA_MAXIMO"),
            )
            .otherwise(lit("NORMAL")),
        )
    )

    produto_keys = dim_produto_df.select(
        "produto_id",
        "produto_sk",
    )

    fact_df = (
        estoque_base
        .join(
            produto_keys,
            "produto_id",
            "inner",
        )
    )

    return fact_df.select(
        col("estoque_id").cast("long"),
        col("produto_sk").cast("long"),
        col("data_sk").cast("long"),
        "local_estoque",
        col("quantidade_estoque").cast("long"),
        col("estoque_minimo").cast("long"),
        col("estoque_maximo").cast("long"),
        col("saldo_vs_minimo").cast("long"),
        "status_estoque",
    )