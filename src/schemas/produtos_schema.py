from pyspark.sql.types import StringType, StructField, StructType

PRODUTOS_SCHEMA = StructType([
    StructField("produto_id", StringType(), True),
    StructField("descricao", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("volume_ml", StringType(), True),
    StructField("tipo_embalagem", StringType(), True),
    StructField("preco_unitario", StringType(), True),
    StructField("custo_unitario", StringType(), True),
    StructField("status_produto", StringType(), True),
])
