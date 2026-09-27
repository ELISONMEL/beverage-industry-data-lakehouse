from pyspark.sql.types import StringType, StructField, StructType

DISTRIBUICAO_SCHEMA = StructType([
    StructField("entrega_id", StringType(), True),
    StructField("pedido_id", StringType(), True),
    StructField("data_saida", StringType(), True),
    StructField("data_prevista", StringType(), True),
    StructField("data_entrega", StringType(), True),
    StructField("transportadora", StringType(), True),
    StructField("rota", StringType(), True),
])
