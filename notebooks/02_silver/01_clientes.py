from src.config.settings import BRONZE_PATH, QUARANTINE_PATH, SILVER_PATH
from src.quality.metrics import calculate_quality_metrics
from src.transformation.clientes_silver import (
    apply_quality_rules,
    deduplicate_clientes,
    split_valid_invalid,
    transform_clientes,
)

df_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/clientes")
df_clean = transform_clientes(df_bronze)
df_dedup = deduplicate_clientes(df_clean)
df_quality = apply_quality_rules(df_dedup)
df_valid, df_invalid = split_valid_invalid(df_quality)

df_valid.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}/clientes")
df_invalid.write.format("delta").mode("append").save(f"{QUARANTINE_PATH}/clientes")

print(calculate_quality_metrics(df_quality, df_valid, df_invalid))
