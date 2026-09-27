from pyspark.sql.functions import col, date_format, dayofmonth, dayofweek, month, quarter, row_number, year
from pyspark.sql.window import Window

# Gold / dimensional model (full-refresh portfolio demo).
clientes = spark.read.format("delta").load("/mnt/datalake/silver/clientes")
produtos = spark.read.format("delta").load("/mnt/datalake/silver/produtos")
vendedores = spark.read.format("delta").load("/mnt/datalake/silver/vendedores")
pedidos = spark.read.format("delta").load("/mnt/datalake/silver/pedidos")
itens = spark.read.format("delta").load("/mnt/datalake/silver/itens_pedido")

dim_cliente = (
    clientes.select("cliente_id", "nome_cliente", "tipo_cliente", "cidade", "estado", "status_cliente")
    .dropDuplicates(["cliente_id"])
    .withColumn("cliente_sk", row_number().over(Window.orderBy("cliente_id")))
)

dim_produto = (
    produtos.select("produto_id", "descricao", "categoria", "volume_ml", "tipo_embalagem", "status_produto")
    .dropDuplicates(["produto_id"])
    .withColumn("produto_sk", row_number().over(Window.orderBy("produto_id")))
)

dim_vendedor = (
    vendedores.select("vendedor_id", "nome_vendedor", "cidade_base", "estado", "status_vendedor")
    .dropDuplicates(["vendedor_id"])
    .withColumn("vendedor_sk", row_number().over(Window.orderBy("vendedor_id")))
)

dim_data = (
    spark.sql("SELECT explode(sequence(to_date('2025-01-01'), to_date('2026-12-31'), interval 1 day)) AS data")
    .withColumn("data_sk", date_format("data", "yyyyMMdd").cast("int"))
    .withColumn("ano", year("data"))
    .withColumn("mes", month("data"))
    .withColumn("dia", dayofmonth("data"))
    .withColumn("trimestre", quarter("data"))
    .withColumn("dia_semana", dayofweek("data"))
)

fact_vendas = (
    itens.join(pedidos, "pedido_id", "inner")
    .join(dim_cliente.select("cliente_id", "cliente_sk"), "cliente_id", "left")
    .join(dim_produto.select("produto_id", "produto_sk"), "produto_id", "left")
    .join(dim_vendedor.select("vendedor_id", "vendedor_sk"), "vendedor_id", "left")
    .withColumn("data_sk", date_format(col("data_pedido"), "yyyyMMdd").cast("int"))
    .select(
        "pedido_id", "item_id", "cliente_sk", "produto_sk", "vendedor_sk", "data_sk",
        "quantidade", "preco_unitario", "percentual_desconto", "valor_total_item",
        "canal_venda", "forma_pagamento", "status_pedido"
    )
)

for name, df in {
    "dim_cliente": dim_cliente,
    "dim_produto": dim_produto,
    "dim_vendedor": dim_vendedor,
    "dim_data": dim_data,
    "fact_vendas": fact_vendas,
}.items():
    df.write.format("delta").mode("overwrite").save(f"/mnt/datalake/gold/{name}")
