from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from src.quality.metrics import calculate_quality_metrics
from src.transformation.itens_pedido_silver import (
    apply_itens_quality,
    transform_itens_pedido,
)


def create_spark_session():

    builder = (
        SparkSession.builder
        .appName("BeverageIndustry-Silver-ItensPedido")
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

    bronze_itens_path = (
        project_root
        / "data"
        / "lakehouse"
        / "bronze"
        / "itens_pedido"
    )

    silver_pedidos_path = (
        project_root
        / "data"
        / "lakehouse"
        / "silver"
        / "pedidos"
    )

    silver_produtos_path = (
        project_root
        / "data"
        / "lakehouse"
        / "silver"
        / "produtos"
    )

    silver_itens_path = (
        project_root
        / "data"
        / "lakehouse"
        / "silver"
        / "itens_pedido"
    )

    quarantine_path = (
        project_root
        / "data"
        / "lakehouse"
        / "quarantine"
        / "itens_pedido"
    )

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    print(
        "\n=== SILVER PIPELINE - ITENS PEDIDO ==="
    )

    # -------------------------------------------------
    # 1. Leitura Bronze
    # -------------------------------------------------

    bronze_df = (
        spark.read
        .format("delta")
        .load(str(bronze_itens_path))
    )

    print(
        f"\nRegistros Bronze: "
        f"{bronze_df.count():,}"
    )

    # -------------------------------------------------
    # 2. Transformação
    # -------------------------------------------------

    transformed_df = transform_itens_pedido(
        bronze_df
    )

    # -------------------------------------------------
    # 3. Deduplicação
    # -------------------------------------------------

    deduplicated_df = (
        transformed_df
        .dropDuplicates(["item_id"])
    )

    print(
        f"Após deduplicação: "
        f"{deduplicated_df.count():,}"
    )

    # -------------------------------------------------
    # 4. Regras próprias do item
    # -------------------------------------------------

    quality_df = apply_itens_quality(
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
    # 5. Integridade referencial com pedidos
    # -------------------------------------------------

    pedidos_df = (
        spark.read
        .format("delta")
        .load(str(silver_pedidos_path))
        .select("pedido_id")
        .distinct()
    )

    itens_pedido_valido_df = (
        valid_quality_df
        .join(
            pedidos_df,
            "pedido_id",
            "left_semi",
        )
    )

    itens_pedido_invalido_df = (
        valid_quality_df
        .join(
            pedidos_df,
            "pedido_id",
            "left_anti",
        )
        .withColumn(
            "_dq_reason",
            lit("PEDIDO_NAO_ENCONTRADO"),
        )
    )

    # -------------------------------------------------
    # 6. Integridade referencial com produtos
    # -------------------------------------------------

    produtos_df = (
        spark.read
        .format("delta")
        .load(str(silver_produtos_path))
        .select("produto_id")
        .distinct()
    )

    itens_validos_df = (
        itens_pedido_valido_df
        .join(
            produtos_df,
            "produto_id",
            "left_semi",
        )
    )

    itens_produto_invalido_df = (
        itens_pedido_valido_df
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

    # -------------------------------------------------
    # 7. Quarantine consolidada
    # -------------------------------------------------

    invalid_df = (
        invalid_quality_df
        .unionByName(
            itens_pedido_invalido_df,
            allowMissingColumns=True,
        )
        .unionByName(
            itens_produto_invalido_df,
            allowMissingColumns=True,
        )
    )

    valid_df = itens_validos_df

    # -------------------------------------------------
    # 8. Métricas
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
    # 9. Escrita Silver
    # -------------------------------------------------

    (
        valid_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(silver_itens_path))
    )

    # -------------------------------------------------
    # 10. Escrita Quarantine
    # -------------------------------------------------

    (
        invalid_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(quarantine_path))
    )

    print("\n=== RESULTADO ===")

    print(
        f"Silver itens_pedido: "
        f"{metrics['valid_records']:,}"
    )

    print(
        f"Quarantine itens_pedido: "
        f"{metrics['invalid_records']:,}"
    )

    print(
        "\nPipeline Silver de "
        "itens_pedido concluído!"
    )

    spark.stop()


if __name__ == "__main__":
    main()