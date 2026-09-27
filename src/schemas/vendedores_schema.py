from pyspark.sql.types import StringType, StructField, StructType

VENDEDORES_SCHEMA = StructType([
    StructField("vendedor_id", StringType(), True),
    StructField("nome_vendedor", StringType(), True),
    StructField("cidade_base", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("data_admissao", StringType(), True),
    StructField("status_vendedor", StringType(), True),
])
