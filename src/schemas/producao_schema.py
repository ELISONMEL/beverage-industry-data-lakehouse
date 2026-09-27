from pyspark.sql.types import StringType, StructField, StructType

PRODUCAO_SCHEMA = StructType([
    StructField("producao_id", StringType(), True),
    StructField("produto_id", StringType(), True),
    StructField("data_producao", StringType(), True),
    StructField("lote", StringType(), True),
    StructField("turno", StringType(), True),
    StructField("quantidade_produzida", StringType(), True),
    StructField("quantidade_descartada", StringType(), True),
    StructField("linha_producao", StringType(), True),
])
