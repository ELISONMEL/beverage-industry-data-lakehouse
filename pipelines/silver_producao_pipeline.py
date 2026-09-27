from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from src.quality.metrics import (
    calculate_quality_metrics,
)
from src.transformation.producao_silver import (
    apply_producao_quality,
    deduplicate_producao,
    transform_producao,
)


def create_spark_session():

    builder = (
        SparkSession.builder
        .appName(
            "BeverageIndustry-Silver-Producao"
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
        / "producao"
    )

    silver_produtos_path = (
        project_root
        / "data"
        / "lakehouse"
        / "silver"
        / "produtos"
    )

    silver_path = (
        project_root
        / "data"
        / "lakehouse"
        / "silver"
        / "producao"
    )

    quarantine_path = (
        project_root
        / "data"
        / "lakehouse"
        / "quarantine"
        / "producao"
    )

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print(
        "\n=== SILVER PIPELINE - PRODUCAO ==="
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
        transform_producao(
            bronze_df
        )
    )

    deduplicated_df = (
        deduplicate_producao(
            transformed_df
        )
    )

    print(
        f"Após deduplicação: "
        f"{deduplicated_df.count():,}"
    )

    quality_df = (
        apply_producao_quality(
            deduplicated_df
        )
    )

    valid_quality_df = (
        quality_df
        .filter("_dq_reason IS NULL")
        .drop("_dq_reason")
    )

    invalid_quality_df = (
        quality_df
        .filter("_dq_reason IS NOT NULL")
    )

    produtos_df = (
        spark.read
        .format("delta")
        .load(str(silver_produtos_path))
        .select("produto_id")
        .distinct()
    )

    valid_df = (
        valid_quality_df
        .join(
            produtos_df,
            "produto_id",
            "left_semi",
        )
    )

    invalid_produto_df = (
        valid_quality_df
        .join(
            produtos_df,
            "produto_id",
            "left_anti",
        )
        .withColumn(
            "_dq_reason",
            lit("PRODUTO_NAO_ENCONTRADO"),
        )
    )

    invalid_df = (
        invalid_quality_df
        .unionByName(
            invalid_produto_df,
            allowMissingColumns=True,
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
        f"Silver producao: "
        f"{metrics['valid_records']:,}"
    )

    print(
        f"Quarantine producao: "
        f"{metrics['invalid_records']:,}"
    )

    print(
        "\nPipeline Silver "
        "de producao concluído!"
    )

    spark.stop()


if __name__ == "__main__":
    main()