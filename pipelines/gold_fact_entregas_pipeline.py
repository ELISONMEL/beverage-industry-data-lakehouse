from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.transformation.fact_entregas import create_fact_entregas


def create_spark_session() -> SparkSession:
    """
    Cria a sessão Spark configurada com Delta Lake.
    """

    builder = (
        SparkSession.builder
        .appName("GoldFactEntregasPipeline")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    silver_distribuicao_path = "data/lakehouse/silver/distribuicao"
    gold_fact_entregas_path = "data/lakehouse/gold/fact_entregas"

    print("\n=== GOLD FACT_ENTREGAS ===")

    distribuicao_df = (
        spark.read
        .format("delta")
        .load(silver_distribuicao_path)
    )

    print("Silver distribuicao:", distribuicao_df.count())

    fact_entregas_df = create_fact_entregas(
        distribuicao_df=distribuicao_df,
    )

    print("Registros fact_entregas:", fact_entregas_df.count())

    (
        fact_entregas_df.write
        .format("delta")
        .mode("overwrite")
        .save(gold_fact_entregas_path)
    )

    gold_df = (
        spark.read
        .format("delta")
        .load(gold_fact_entregas_path)
    )

    print("Gold fact_entregas:", gold_df.count())

    print("\n=== AMOSTRA FACT_ENTREGAS ===")
    gold_df.show(10, False)

    print("\nPipeline Gold fact_entregas concluído!")

    spark.stop()


if __name__ == "__main__":
    main()