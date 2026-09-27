from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    dayofweek,
    lit,
    month,
    quarter,
    when,
    year,
)


def create_dim_data(df: DataFrame) -> DataFrame:
    """
    Cria a dimensão calendário para a camada Gold.
    """

    return (
        df
        .select(
            col("data")
        )
        .dropDuplicates(["data"])
        .withColumn(
            "data_sk",
            date_format(
                col("data"),
                "yyyyMMdd",
            ).cast("long"),
        )
        .withColumn(
            "ano",
            year(col("data")),
        )
        .withColumn(
            "mes",
            month(col("data")),
        )
        .withColumn(
            "nome_mes",
            when(month(col("data")) == 1, lit("JANEIRO"))
            .when(month(col("data")) == 2, lit("FEVEREIRO"))
            .when(month(col("data")) == 3, lit("MARCO"))
            .when(month(col("data")) == 4, lit("ABRIL"))
            .when(month(col("data")) == 5, lit("MAIO"))
            .when(month(col("data")) == 6, lit("JUNHO"))
            .when(month(col("data")) == 7, lit("JULHO"))
            .when(month(col("data")) == 8, lit("AGOSTO"))
            .when(month(col("data")) == 9, lit("SETEMBRO"))
            .when(month(col("data")) == 10, lit("OUTUBRO"))
            .when(month(col("data")) == 11, lit("NOVEMBRO"))
            .when(month(col("data")) == 12, lit("DEZEMBRO")),
        )
        .withColumn(
            "trimestre",
            quarter(col("data")),
        )
        .withColumn(
            "dia",
            dayofmonth(col("data")),
        )
        .withColumn(
            "dia_semana",
            dayofweek(col("data")),
        )
        .withColumn(
            "nome_dia_semana",
            when(dayofweek(col("data")) == 1, lit("DOMINGO"))
            .when(dayofweek(col("data")) == 2, lit("SEGUNDA"))
            .when(dayofweek(col("data")) == 3, lit("TERCA"))
            .when(dayofweek(col("data")) == 4, lit("QUARTA"))
            .when(dayofweek(col("data")) == 5, lit("QUINTA"))
            .when(dayofweek(col("data")) == 6, lit("SEXTA"))
            .when(dayofweek(col("data")) == 7, lit("SABADO")),
        )
        .withColumn(
            "fim_de_semana",
            when(
                dayofweek(col("data")).isin(1, 7),
                lit(True),
            ).otherwise(lit(False)),
        )
        .select(
            "data_sk",
            "data",
            "ano",
            "mes",
            "nome_mes",
            "trimestre",
            "dia",
            "dia_semana",
            "nome_dia_semana",
            "fim_de_semana",
        )
    )