from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.transformation.dim_vendedor import create_dim_vendedor


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("BeverageIndustry-Gold-DimVendedor")
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

    silver_path = (
        project_root
        / "data"
        / "lakehouse"
        / "silver"
        / "vendedores"
    )

    gold_path = (
        project_root
        / "data"
        / "lakehouse"
        / "gold"
        / "dim_vendedor"
    )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== GOLD PIPELINE - DIM_VENDEDOR ===")

    silver_df = (
        spark.read
        .format("delta")
        .load(str(silver_path))
    )

    print(
        f"\nRegistros Silver vendedores: "
        f"{silver_df.count():,}"
    )

    dim_vendedor_df = create_dim_vendedor(
        silver_df
    )

    print(
        f"Registros dim_vendedor: "
        f"{dim_vendedor_df.count():,}"
    )

    (
        dim_vendedor_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(gold_path))
    )

    print("\n=== RESULTADO ===")

    print(
        f"Gold dim_vendedor: "
        f"{dim_vendedor_df.count():,}"
    )

    print(
        "\nPipeline Gold dim_vendedor concluído!"
    )

    spark.stop()


if __name__ == "__main__":
    main()