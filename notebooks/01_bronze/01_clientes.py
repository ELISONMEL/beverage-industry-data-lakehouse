from src.config.settings import BRONZE_PATH, LANDING_PATH
from src.ingestion.csv_to_bronze import ingest_csv_to_bronze
from src.schemas.clientes_schema import CLIENTES_SCHEMA

result = ingest_csv_to_bronze(
    spark=spark,
    source_path=f"{LANDING_PATH}/clientes/",
    destination_path=f"{BRONZE_PATH}/clientes/",
    schema=CLIENTES_SCHEMA,
)

print(result)
