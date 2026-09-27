from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number


def create_dim_cliente(df: DataFrame) -> DataFrame:
    """
    Cria a dimensão de clientes para a camada Gold.

    A dimensão mantém apenas atributos relevantes
    para análise de negócio e cria uma chave substituta
    (surrogate key) chamada cliente_sk.
    """

    window_cliente = Window.orderBy("cliente_id")

    return (
        df
        .select(
            "cliente_id",
            "nome_cliente",
            "tipo_cliente",
            "cidade",
            "estado",
            "data_cadastro",
            "status_cliente",
        )
        .dropDuplicates(["cliente_id"])
        .withColumn(
            "cliente_sk",
            row_number().over(window_cliente),
        )
        .select(
            col("cliente_sk").cast("long"),
            col("cliente_id").cast("long"),
            "nome_cliente",
            "tipo_cliente",
            "cidade",
            "estado",
            "data_cadastro",
            "status_cliente",
        )
    )