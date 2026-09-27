from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number


def create_dim_produto(df: DataFrame) -> DataFrame:
    """
    Cria a dimensão de produtos para a camada Gold.
    """

    window_produto = Window.orderBy("produto_id")

    return (
        df
        .select(
            "produto_id",
            "descricao",
            "categoria",
            "volume_ml",
            "tipo_embalagem",
            "preco_unitario",
            "custo_unitario",
            "status_produto",
        )
        .dropDuplicates(["produto_id"])
        .withColumn(
            "produto_sk",
            row_number().over(window_produto),
        )
        .select(
            col("produto_sk").cast("long"),
            col("produto_id").cast("long"),
            "descricao",
            "categoria",
            "volume_ml",
            "tipo_embalagem",
            "preco_unitario",
            "custo_unitario",
            "status_produto",
        )
    )