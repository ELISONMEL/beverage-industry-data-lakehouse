from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.transformation.fact_estoque import create_fact_estoque


def create_spark_session() -> SparkSession:
    """
    Cria a sessão Spark configurada com Delta Lake.
    """

    builder = (
        SparkSession.builder
        .appName("GoldFactEstoquePipeline")
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

    silver_estoque_path = "data/lakehouse/silver/estoque"
    dim_produto_path = "data/lakehouse/gold/dim_produto"
    gold_fact_estoque_path = "data/lakehouse/gold/fact_estoque"

    print("\n=== GOLD FACT_ESTOQUE ===")

    estoque_df = (
        spark.read
        .format("delta")
        .load(silver_estoque_path)
    )

    dim_produto_df = (
        spark.read
        .format("delta")
        .load(dim_produto_path)
    )

    print("Silver estoque:", estoque_df.count())
    print("Dim produto:", dim_produto_df.count())

    fact_estoque_df = create_fact_estoque(
        estoque_df=estoque_df,
        dim_produto_df=dim_produto_df,
    )

    print("Registros fact_estoque:", fact_estoque_df.count())

    (
        fact_estoque_df.write
        .format("delta")
        .mode("overwrite")
        .save(gold_fact_estoque_path)
    )

    gold_df = (
        spark.read
        .format("delta")
        .load(gold_fact_estoque_path)
    )

    print("Gold fact_estoque:", gold_df.count())

    print("\n=== AMOSTRA FACT_ESTOQUE ===")
    gold_df.show(10, False)

    print("\nPipeline Gold fact_estoque concluído!")

    spark.stop()


if __name__ == "__main__":
    main()