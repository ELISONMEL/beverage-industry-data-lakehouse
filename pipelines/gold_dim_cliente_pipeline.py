from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.transformation.dim_cliente import create_dim_cliente


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("BeverageIndustry-Gold-DimCliente")
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
        / "clientes"
    )

    gold_path = (
        project_root
        / "data"
        / "lakehouse"
        / "gold"
        / "dim_cliente"
    )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== GOLD PIPELINE - DIM_CLIENTE ===")

    silver_df = (
        spark.read
        .format("delta")
        .load(str(silver_path))
    )

    print(
        f"\nRegistros Silver clientes: "
        f"{silver_df.count():,}"
    )

    dim_cliente_df = create_dim_cliente(
        silver_df
    )

    print(
        f"Registros dim_cliente: "
        f"{dim_cliente_df.count():,}"
    )

    (
        dim_cliente_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(gold_path))
    )

    print("\n=== RESULTADO ===")

    print(
        f"Gold dim_cliente: "
        f"{dim_cliente_df.count():,}"
    )

    print(
        "\nPipeline Gold dim_cliente concluído!"
    )

    spark.stop()


if __name__ == "__main__":
    main()