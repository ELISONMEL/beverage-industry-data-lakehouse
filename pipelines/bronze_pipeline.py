from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.ingestion.csv_to_bronze import ingest_csv_to_bronze
from src.schemas.clientes_schema import CLIENTES_SCHEMA
from src.schemas.distribuicao_schema import DISTRIBUICAO_SCHEMA
from src.schemas.estoque_schema import ESTOQUE_SCHEMA
from src.schemas.itens_pedido_schema import ITENS_PEDIDO_SCHEMA
from src.schemas.pedidos_schema import PEDIDOS_SCHEMA
from src.schemas.producao_schema import PRODUCAO_SCHEMA
from src.schemas.produtos_schema import PRODUTOS_SCHEMA
from src.schemas.vendedores_schema import VENDEDORES_SCHEMA

DATASETS = {
    "clientes": CLIENTES_SCHEMA,
    "produtos": PRODUTOS_SCHEMA,
    "vendedores": VENDEDORES_SCHEMA,
    "pedidos": PEDIDOS_SCHEMA,
    "itens_pedido": ITENS_PEDIDO_SCHEMA,
    "producao": PRODUCAO_SCHEMA,
    "estoque": ESTOQUE_SCHEMA,
    "distribuicao": DISTRIBUICAO_SCHEMA,
}


def create_spark_session():

    builder = (
        SparkSession.builder
        .appName("BeverageIndustry-Bronze")
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


def process_dataset(
    spark,
    project_root,
    dataset_name,
    schema,
):

    source = (
        project_root
        / "data"
        / f"{dataset_name}.csv"
    )

    destination = (
        project_root
        / "data"
        / "lakehouse"
        / "bronze"
        / dataset_name
    )

    audit_path = (
        project_root
        / "data"
        / "lakehouse"
        / "audit"
        / "processed_files"
    )

    print("\n" + "=" * 60)
    print(f"DATASET: {dataset_name}")
    print("=" * 60)

    print(f"Origem: {source}")
    print(f"Destino: {destination}")

    result = ingest_csv_to_bronze(
        spark=spark,
        source_path=str(source),
        destination_path=str(destination),
        audit_path=str(audit_path),
        schema=schema,
    )

    print(
        f"Status: {result['status']}"
    )

    print(
        f"Registros processados: "
        f"{result['records']:,}"
    )

    print(
        f"Batch ID: "
        f"{result['batch_id']}"
    )

    if "message" in result:
        print(
            f"Mensagem: "
            f"{result['message']}"
        )

    if destination.exists():

        bronze_df = (
            spark.read
            .format("delta")
            .load(str(destination))
        )

        total = bronze_df.count()

        print(
            f"Total Bronze: "
            f"{total:,}"
        )


def main():

    project_root = (
        Path(__file__)
        .resolve()
        .parents[1]
    )

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print(
        "\n=== BEVERAGE INDUSTRY "
        "BRONZE PIPELINE ==="
    )

    for dataset_name, schema in DATASETS.items():

        try:

            process_dataset(
                spark=spark,
                project_root=project_root,
                dataset_name=dataset_name,
                schema=schema,
            )

        except Exception as error: # noqa: BLE001

            print(
                f"\nERRO no dataset "
                f"{dataset_name}:"
            )

            print(error)

    print(
        "\n=== PIPELINE BRONZE "
        "FINALIZADO ==="
    )


if __name__ == "__main__":
    main()