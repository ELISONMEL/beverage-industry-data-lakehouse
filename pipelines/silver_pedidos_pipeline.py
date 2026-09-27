from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from src.quality.metrics import calculate_quality_metrics
from src.transformation.pedidos_silver import (
    apply_pedidos_quality,
    transform_pedidos,
)


def create_spark_session():

    builder = (
        SparkSession.builder
        .appName("BeverageIndustry-Silver-Pedidos")
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

    bronze_pedidos_path = (
        project_root
        / "data"
        / "lakehouse"
        / "bronze"
        / "pedidos"
    )

    silver_clientes_path = (
        project_root
        / "data"
        / "lakehouse"
        / "silver"
        / "clientes"
    )

    silver_pedidos_path = (
        project_root
        / "data"
        / "lakehouse"
        / "silver"
        / "pedidos"
    )

    quarantine_path = (
        project_root
        / "data"
        / "lakehouse"
        / "quarantine"
        / "pedidos"
    )

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    print("\n=== SILVER PIPELINE - PEDIDOS ===")

    # -------------------------------------------------
    # 1. Bronze
    # -------------------------------------------------

    bronze_df = (
        spark.read
        .format("delta")
        .load(str(bronze_pedidos_path))
    )

    print(
        f"\nRegistros Bronze: "
        f"{bronze_df.count():,}"
    )

    # -------------------------------------------------
    # 2. Transformação
    # -------------------------------------------------

    transformed_df = transform_pedidos(
        bronze_df
    )

    # -------------------------------------------------
    # 3. Deduplicação
    # -------------------------------------------------

    deduplicated_df = (
        transformed_df
        .dropDuplicates(["pedido_id"])
    )

    print(
        f"Após deduplicação: "
        f"{deduplicated_df.count():,}"
    )

    # -------------------------------------------------
    # 4. Regras de Data Quality
    # -------------------------------------------------

    quality_df = apply_pedidos_quality(
        deduplicated_df
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

    # -------------------------------------------------
    # 5. Integridade referencial com clientes
    # -------------------------------------------------

    silver_clientes_df = (
        spark.read
        .format("delta")
        .load(str(silver_clientes_path))
        .select("cliente_id")
        .distinct()
    )

    referential_valid_df = (
        valid_quality_df
        .join(
            silver_clientes_df,
            "cliente_id",
            "left_semi",
        )
    )

    referential_invalid_df = (
        valid_quality_df
        .join(
            silver_clientes_df,
            "cliente_id",
            "left_anti",
        )
        .withColumn(
            "_dq_reason",
            lit("CLIENTE_NAO_ENCONTRADO"),
        )
    )

    # -------------------------------------------------
    # 6. Quarantine consolidada
    # -------------------------------------------------

    invalid_df = (
        invalid_quality_df
        .unionByName(
            referential_invalid_df,
            allowMissingColumns=True,
        )
    )

    valid_df = referential_valid_df

    # -------------------------------------------------
    # 7. Métricas
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
    # 8. Escrita Silver
    # -------------------------------------------------

    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(silver_pedidos_path))
    )

    # -------------------------------------------------
    # 9. Escrita Quarantine
    # -------------------------------------------------

    (
        invalid_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(quarantine_path))
    )

    print("\n=== RESULTADO ===")

    print(
        f"Silver pedidos: "
        f"{metrics['valid_records']:,}"
    )

    print(
        f"Quarantine pedidos: "
        f"{metrics['invalid_records']:,}"
    )

    print(
        "\nPipeline Silver de pedidos concluído!"
    )

    spark.stop()


if __name__ == "__main__":
    main()
