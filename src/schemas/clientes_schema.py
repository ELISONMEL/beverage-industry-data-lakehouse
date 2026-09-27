from pyspark.sql.types import StringType, StructField, StructType

CLIENTES_SCHEMA = StructType([
    StructField("cliente_id", StringType(), True),
    StructField("nome_cliente", StringType(), True),
    StructField("tipo_cliente", StringType(), True),
    StructField("documento", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("data_cadastro", StringType(), True),
    StructField("status_cliente", StringType(), True),
])
