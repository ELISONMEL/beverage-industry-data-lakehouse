from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.quality.metrics import calculate_quality_metrics
from src.transformation.clientes_silver import (
    apply_quality_rules,
    deduplicate_clientes,
    split_valid_invalid,
    transform_clientes,
)


def create_spark_session():

    builder = (
        SparkSession.builder
        .appName("BeverageIndustry-Silver")
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
        / "clientes"
    )

    silver_path = (
        project_root
        / "data"
        / "lakehouse"
        / "silver"
        / "clientes"
    )

    quarantine_path = (
        project_root
        / "data"
        / "lakehouse"
        / "quarantine"
        / "clientes"
    )

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    print("\n=== SILVER PIPELINE - CLIENTES ===")

    # -------------------------------------------------
    # 1. Leitura da Bronze
    # -------------------------------------------------

    bronze_df = (
        spark.read
        .format("delta")
        .load(str(bronze_path))
    )

    print(
        f"\nRegistros Bronze: "
        f"{bronze_df.count():,}"
    )

    # -------------------------------------------------
    # 2. Transformação
    # -------------------------------------------------

    transformed_df = transform_clientes(
        bronze_df
    )

    # -------------------------------------------------
    # 3. Deduplicação
    # -------------------------------------------------

    deduplicated_df = deduplicate_clientes(
        transformed_df
    )

    # -------------------------------------------------
    # 4. Regras de Data Quality
    # -------------------------------------------------

    quality_df = apply_quality_rules(
        deduplicated_df
    )

    # -------------------------------------------------
    # 5. Separação válidos / inválidos
    # -------------------------------------------------

    valid_df, invalid_df = split_valid_invalid(
        quality_df
    )

    # -------------------------------------------------
    # 6. Métricas
    # -------------------------------------------------

    metrics = calculate_quality_metrics(
        deduplicated_df,
        valid_df,
        invalid_df,
    )

    print("\n=== DATA QUALITY ===")

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

    # -------------------------------------------------
    # 7. Escrita Silver
    # -------------------------------------------------

    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(silver_path))
    )

    # -------------------------------------------------
    # 8. Escrita Quarantine
    # -------------------------------------------------

    (
        invalid_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(quarantine_path))
    )

    print("\n=== RESULTADO ===")

    print(
        f"Silver: "
        f"{metrics['valid_records']:,}"
    )

    print(
        f"Quarantine: "
        f"{metrics['invalid_records']:,}"
    )

    print(
        "\nPipeline Silver concluído!"
    )

    spark.stop()


if __name__ == "__main__":
    main()
