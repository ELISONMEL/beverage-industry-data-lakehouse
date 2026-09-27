from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.quality.metrics import (
    calculate_quality_metrics,
)
from src.transformation.produtos_silver import (
    apply_produtos_quality,
    deduplicate_produtos,
    split_valid_invalid,
    transform_produtos,
)


def create_spark_session():

    builder = (
        SparkSession.builder
        .appName(
            "BeverageIndustry-Silver-Produtos"
        )
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

    project_root = (
        Path(__file__)
        .resolve()
        .parents[1]
    )

    bronze_path = (
        project_root
        / "data"
        / "lakehouse"
        / "bronze"
        / "produtos"
    )

    silver_path = (
        project_root
        / "data"
        / "lakehouse"
        / "silver"
        / "produtos"
    )

    quarantine_path = (
        project_root
        / "data"
        / "lakehouse"
        / "quarantine"
        / "produtos"
    )

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print(
        "\n=== SILVER PIPELINE - PRODUTOS ==="
    )

    bronze_df = (
        spark.read
        .format("delta")
        .load(str(bronze_path))
    )

    print(
        f"\nRegistros Bronze: "
        f"{bronze_df.count():,}"
    )

    transformed_df = (
        transform_produtos(
            bronze_df
        )
    )

    deduplicated_df = (
        deduplicate_produtos(
            transformed_df
        )
    )

    quality_df = (
        apply_produtos_quality(
            deduplicated_df
        )
    )

    valid_df, invalid_df = (
        split_valid_invalid(
            quality_df
        )
    )

    metrics = (
        calculate_quality_metrics(
            deduplicated_df,
            valid_df,
            invalid_df,
        )
    )

    print(
        "\n=== DATA QUALITY ==="
    )

    print(
        f"Total analisado: "
        f"{metrics['total_records']:,}"
    )

    print(
        f"Registros válidos: "
        f"{metrics['valid_records']:,}"
    )

    print(
        f"Registros inválidos: "
        f"{metrics['invalid_records']:,}"
    )

    print(
        f"Taxa de qualidade: "
        f"{metrics['quality_rate']}%"
    )

    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(silver_path))
    )

    (
        invalid_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(quarantine_path))
    )

    print(
        "\n=== RESULTADO ==="
    )

    print(
        f"Silver produtos: "
        f"{metrics['valid_records']:,}"
    )

    print(
        f"Quarantine produtos: "
        f"{metrics['invalid_records']:,}"
    )

    print(
        "\nPipeline Silver "
        "de produtos concluído!"
    )

    spark.stop()


if __name__ == "__main__":
    main()