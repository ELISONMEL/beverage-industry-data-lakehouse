from pyspark.sql import DataFrame
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import col, lit, to_date, trim, upper, when
from pyspark.sql.functions import round as spark_round


def transform_pedidos(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("pedido_id", col("pedido_id").cast("long"))
        .withColumn("cliente_id", col("cliente_id").cast("long"))
        .withColumn("vendedor_id", col("vendedor_id").cast("long"))
        .withColumn("data_pedido", to_date(col("data_pedido"), "yyyy-MM-dd"))
        .withColumn("canal_venda", upper(trim(col("canal_venda"))))
        .withColumn("status_pedido", upper(trim(col("status_pedido"))))
        .withColumn("forma_pagamento", upper(trim(col("forma_pagamento"))))
        .withColumn("valor_bruto", spark_round(col("valor_bruto").cast("double"), 2))
        .withColumn("valor_desconto", spark_round(col("valor_desconto").cast("double"), 2))
        .withColumn("valor_total", spark_round(col("valor_total").cast("double"), 2))
    )


def apply_pedidos_quality(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "_dq_reason",
        when(col("pedido_id").isNull(), lit("PEDIDO_ID_NULL"))
        .when(col("cliente_id").isNull(), lit("CLIENTE_ID_NULL"))
        .when(col("vendedor_id").isNull(), lit("VENDEDOR_ID_NULL"))
        .when(col("data_pedido").isNull(), lit("DATA_PEDIDO_INVALIDA"))
        .when(col("valor_bruto").isNull(), lit("VALOR_BRUTO_NULL"))
        .when(col("valor_bruto") < 0, lit("VALOR_BRUTO_NEGATIVO"))
        .when(col("valor_desconto").isNull(), lit("DESCONTO_NULL"))
        .when(col("valor_desconto") < 0, lit("DESCONTO_NEGATIVO"))
        .when(col("valor_desconto") > col("valor_bruto"), lit("DESCONTO_MAIOR_QUE_BRUTO"))
        .when(col("valor_total").isNull(), lit("VALOR_TOTAL_NULL"))
        .when(col("valor_total") < 0, lit("VALOR_TOTAL_NEGATIVO"))
        .when(
            spark_abs(col("valor_total") - (col("valor_bruto") - col("valor_desconto"))) > 0.01,
            lit("VALOR_TOTAL_INCONSISTENTE"),
        )
    )
