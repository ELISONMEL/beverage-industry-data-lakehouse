from pyspark.sql.types import StringType, StructField, StructType

ITENS_PEDIDO_SCHEMA = StructType([
    StructField("item_id", StringType(), True),
    StructField("pedido_id", StringType(), True),
    StructField("produto_id", StringType(), True),
    StructField("quantidade", StringType(), True),
    StructField("preco_unitario", StringType(), True),
    StructField("percentual_desconto", StringType(), True),
    StructField("valor_total_item", StringType(), True),
])
