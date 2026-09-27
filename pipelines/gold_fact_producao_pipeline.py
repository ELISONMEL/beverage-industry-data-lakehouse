from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.transformation.fact_producao import create_fact_producao


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("BeverageIndustry-Gold-FactProducao")
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(
        builder
    ).getOrCreate()


def main():
    project_root = Path(__file__).resolve().parents[1]

    silver_producao_path = (
        project_root
        / "data"
        / "lakehouse"
        / "silver"
        / "producao"
    )

    dim_produto_path = (
        project_root
        / "data"
        / "lakehouse"
        / "gold"
        / "dim_produto"
    )

    fact_producao_path = (
        project_root
        / "data"
        / "lakehouse"
        / "gold"
        / "fact_producao"
    )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== GOLD PIPELINE - FACT_PRODUCAO ===")

    producao_df = (
        spark.read
        .format("delta")
        .load(str(silver_producao_path))
    )

    dim_produto_df = (
        spark.read
        .format("delta")
        .load(str(dim_produto_path))
    )

    print(
        f"\nSilver producao: "
        f"{producao_df.count():,}"
    )

    fact_producao_df = create_fact_producao(
        producao_df,
        dim_produto_df,
    )

    print(
        f"Registros fact_producao: "
        f"{fact_producao_df.count():,}"
    )

    (
        fact_producao_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(fact_producao_path))
    )

    print("\n=== RESULTADO ===")

    print(
        f"Gold fact_producao: "
        f"{fact_producao_df.count():,}"
    )

    print(
        "\nPipeline Gold fact_producao concluído!"
    )

    spark.stop()


if __name__ == "__main__":
    main()