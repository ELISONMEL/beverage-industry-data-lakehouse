from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.transformation.fact_vendas import create_fact_vendas


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("BeverageIndustry-Gold-FactVendas")
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

    silver_pedidos_path = (
        project_root
        / "data"
        / "lakehouse"
        / "silver"
        / "pedidos"
    )

    silver_itens_path = (
        project_root
        / "data"
        / "lakehouse"
        / "silver"
        / "itens_pedido"
    )

    dim_cliente_path = (
        project_root
        / "data"
        / "lakehouse"
        / "gold"
        / "dim_cliente"
    )

    dim_produto_path = (
        project_root
        / "data"
        / "lakehouse"
        / "gold"
        / "dim_produto"
    )

    dim_vendedor_path = (
        project_root
        / "data"
        / "lakehouse"
        / "gold"
        / "dim_vendedor"
    )

    fact_vendas_path = (
        project_root
        / "data"
        / "lakehouse"
        / "gold"
        / "fact_vendas"
    )

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("\n=== GOLD PIPELINE - FACT_VENDAS ===")

    pedidos_df = (
        spark.read
        .format("delta")
        .load(str(silver_pedidos_path))
    )

    itens_df = (
        spark.read
        .format("delta")
        .load(str(silver_itens_path))
    )

    dim_cliente_df = (
        spark.read
        .format("delta")
        .load(str(dim_cliente_path))
    )

    dim_produto_df = (
        spark.read
        .format("delta")
        .load(str(dim_produto_path))
    )

    dim_vendedor_df = (
        spark.read
        .format("delta")
        .load(str(dim_vendedor_path))
    )

    print(
        f"\nSilver pedidos: "
        f"{pedidos_df.count():,}"
    )

    print(
        f"Silver itens_pedido: "
        f"{itens_df.count():,}"
    )

    fact_vendas_df = create_fact_vendas(
        pedidos_df,
        itens_df,
        dim_cliente_df,
        dim_produto_df,
        dim_vendedor_df,
    )

    print(
        f"Registros fact_vendas: "
        f"{fact_vendas_df.count():,}"
    )

    (
        fact_vendas_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(fact_vendas_path))
    )

    print("\n=== RESULTADO ===")

    print(
        f"Gold fact_vendas: "
        f"{fact_vendas_df.count():,}"
    )

    print(
        "\nPipeline Gold fact_vendas concluído!"
    )

    spark.stop()


if __name__ == "__main__":
    main()