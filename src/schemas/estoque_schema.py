from pyspark.sql.types import StringType, StructField, StructType

ESTOQUE_SCHEMA = StructType([
    StructField("estoque_id", StringType(), True),
    StructField("produto_id", StringType(), True),
    StructField("data_referencia", StringType(), True),
    StructField("quantidade_estoque", StringType(), True),
    StructField("estoque_minimo", StringType(), True),
    StructField("estoque_maximo", StringType(), True),
    StructField("local_estoque", StringType(), True),
])
