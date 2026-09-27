from pyspark.sql.types import StringType, StructField, StructType

PEDIDOS_SCHEMA = StructType([
    StructField("pedido_id", StringType(), True),
    StructField("cliente_id", StringType(), True),
    StructField("vendedor_id", StringType(), True),
    StructField("data_pedido", StringType(), True),
    StructField("canal_venda", StringType(), True),
    StructField("status_pedido", StringType(), True),
    StructField("forma_pagamento", StringType(), True),
    StructField("valor_bruto", StringType(), True),
    StructField("valor_desconto", StringType(), True),
    StructField("valor_total", StringType(), True),
])
