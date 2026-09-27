from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number


def create_dim_vendedor(df: DataFrame) -> DataFrame:
    """
    Cria a dimensão de vendedores para a camada Gold.
    """

    window_vendedor = Window.orderBy("vendedor_id")

    return (
        df
        .select(
            "vendedor_id",
            "nome_vendedor",
            "cidade_base",
            "estado",
            "data_admissao",
            "status_vendedor",
        )
        .dropDuplicates(["vendedor_id"])
        .withColumn(
            "vendedor_sk",
            row_number().over(window_vendedor),
        )
        .select(
            col("vendedor_sk").cast("long"),
            col("vendedor_id").cast("long"),
            "nome_vendedor",
            "cidade_base",
            "estado",
            "data_admissao",
            "status_vendedor",
        )
    )