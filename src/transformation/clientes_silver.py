from pyspark.sql import DataFrame
from pyspark.sql.functions import col, initcap, lit, to_date, trim, upper, when


def transform_clientes(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("cliente_id", col("cliente_id").cast("long"))
        .withColumn("nome_cliente", initcap(trim(col("nome_cliente"))))
        .withColumn("tipo_cliente", upper(trim(col("tipo_cliente"))))
        .withColumn("cidade", initcap(trim(col("cidade"))))
        .withColumn("estado", upper(trim(col("estado"))))
        .withColumn("status_cliente", upper(trim(col("status_cliente"))))
        .withColumn("data_cadastro", to_date(col("data_cadastro"), "yyyy-MM-dd"))
    )


def deduplicate_clientes(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["cliente_id"])


def apply_quality_rules(df: DataFrame) -> DataFrame:
    tipos_validos = ["SUPERMERCADO", "DISTRIBUIDOR", "RESTAURANTE", "MERCADO", "EMPRESA"]
    status_validos = ["ATIVO", "INATIVO"]

    return df.withColumn(
        "_dq_reason",
        when(col("cliente_id").isNull(), lit("CLIENTE_ID_NULL"))
        .when(
            col("nome_cliente").isNull() | (trim(col("nome_cliente")) == ""),
            lit("NOME_CLIENTE_NULL"),
        )
        .when(col("estado").isNull() | (trim(col("estado")) == ""), lit("ESTADO_NULL"))
        .when(~col("estado").rlike("^[A-Z]{2}$"), lit("ESTADO_INVALIDO"))
        .when(
            col("status_cliente").isNull() | (~col("status_cliente").isin(status_validos)),
            lit("STATUS_INVALIDO"),
        )
        .when(
            col("tipo_cliente").isNull() | (~col("tipo_cliente").isin(tipos_validos)),
            lit("TIPO_CLIENTE_INVALIDO"),
        )
        .when(col("data_cadastro").isNull(), lit("DATA_CADASTRO_INVALIDA")),
    )


def split_valid_invalid(df: DataFrame):
    valid = df.filter(col("_dq_reason").isNull()).drop("_dq_reason")
    invalid = df.filter(col("_dq_reason").isNotNull())
    return valid, invalid
