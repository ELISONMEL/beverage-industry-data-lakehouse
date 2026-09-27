from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, lit, sequence, to_date

from src.transformation.dim_data import create_dim_data


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("BeverageIndustry-Gold-DimData")
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

    gold_path = (
        project_root
        / "data"
        / "lakehouse"
        / "gold"
        / "dim_data"
    )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== GOLD PIPELINE - DIM_DATA ===")

    data_inicial = "2025-01-01"
    data_final = "2026-08-11"

    calendario_base_df = (
        spark.range(1)
        .select(
            explode(
                sequence(
                    to_date(lit(data_inicial)),
                    to_date(lit(data_final)),
                )
            ).alias("data")
        )
    )

    print(
        f"\nDatas geradas no calendário: "
        f"{calendario_base_df.count():,}"
    )

    dim_data_df = create_dim_data(
        calendario_base_df
    )

    print(
        f"Registros dim_data: "
        f"{dim_data_df.count():,}"
    )

    (
        dim_data_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(gold_path))
    )

    print("\n=== RESULTADO ===")

    print(
        f"Gold dim_data: "
        f"{dim_data_df.count():,}"
    )

    print(
        "\nPipeline Gold dim_data concluído!"
    )

    spark.stop()


if __name__ == "__main__":
    main()