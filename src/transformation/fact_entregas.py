from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_format,
    datediff,
    lit,
    when,
)


def create_fact_entregas(
    distribuicao_df: DataFrame,
) -> DataFrame:
    """
    Cria a fato de entregas da camada Gold.

    Grão:
    uma linha por entrega.

    Neste conjunto de dados existe uma relação
    1:1 entre pedido e entrega.
    """

    return (
        distribuicao_df
        .select(
            "entrega_id",
            "pedido_id",
            "data_saida",
            "data_prevista",
            "data_entrega",
            "transportadora",
            "rota",
        )
        .withColumn(
            "data_saida_sk",
            date_format(
                col("data_saida"),
                "yyyyMMdd",
            ).cast("long"),
        )
        .withColumn(
            "data_prevista_sk",
            date_format(
                col("data_prevista"),
                "yyyyMMdd",
            ).cast("long"),
        )
        .withColumn(
            "data_entrega_sk",
            date_format(
                col("data_entrega"),
                "yyyyMMdd",
            ).cast("long"),
        )
        .withColumn(
            "dias_transporte",
            datediff(
                col("data_entrega"),
                col("data_saida"),
            ),
        )
        .withColumn(
            "dias_atraso",
            when(
                col("data_entrega") > col("data_prevista"),
                datediff(
                    col("data_entrega"),
                    col("data_prevista"),
                ),
            ).otherwise(lit(0)),
        )
        .withColumn(
            "status_entrega",
            when(
                col("data_entrega") > col("data_prevista"),
                lit("ATRASADA"),
            ).otherwise(lit("NO_PRAZO")),
        )
        .select(
            col("entrega_id").cast("long"),
            col("pedido_id").cast("long"),
            col("data_saida_sk").cast("long"),
            col("data_prevista_sk").cast("long"),
            col("data_entrega_sk").cast("long"),
            "transportadora",
            "rota",
            col("dias_transporte").cast("integer"),
            col("dias_atraso").cast("integer"),
            "status_entrega",
        )
    )