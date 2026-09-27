from pyspark.sql import DataFrame
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import col, lit, when
from pyspark.sql.functions import round as spark_round


def transform_itens_pedido(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("item_id", col("item_id").cast("long"))
        .withColumn("pedido_id", col("pedido_id").cast("long"))
        .withColumn("produto_id", col("produto_id").cast("long"))
        .withColumn("quantidade", col("quantidade").cast("integer"))
        .withColumn("preco_unitario", col("preco_unitario").cast("double"))
        .withColumn("percentual_desconto", col("percentual_desconto").cast("double"))
        .withColumn("valor_total_item", col("valor_total_item").cast("double"))
        .withColumn(
            "valor_calculado",
            spark_round(
                col("quantidade") * col("preco_unitario") * (1 - col("percentual_desconto") / 100),
                2,
            ),
        )
    )


def apply_itens_quality(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "_dq_reason",
        when(col("item_id").isNull(), lit("ITEM_ID_NULL"))
        .when(col("pedido_id").isNull(), lit("PEDIDO_ID_NULL"))
        .when(col("produto_id").isNull(), lit("PRODUTO_ID_NULL"))
        .when(col("quantidade").isNull(), lit("QUANTIDADE_NULL"))
        .when(col("quantidade") <= 0, lit("QUANTIDADE_INVALIDA"))
        .when(col("preco_unitario").isNull(), lit("PRECO_NULL"))
        .when(col("preco_unitario") <= 0, lit("PRECO_INVALIDO"))
        .when(col("percentual_desconto").isNull(), lit("DESCONTO_NULL"))
        .when(
            (col("percentual_desconto") < 0) | (col("percentual_desconto") > 100),
            lit("DESCONTO_INVALIDO"),
        )
        .when(col("valor_total_item").isNull(), lit("VALOR_TOTAL_ITEM_NULL"))
        .when(
            spark_abs(col("valor_calculado") - col("valor_total_item")) > 0.01,
            lit("VALOR_ITEM_INCONSISTENTE"),
        ),
    )
