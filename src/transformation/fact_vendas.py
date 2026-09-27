from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format


def create_fact_vendas(
    pedidos_df: DataFrame,
    itens_df: DataFrame,
    dim_cliente_df: DataFrame,
    dim_produto_df: DataFrame,
    dim_vendedor_df: DataFrame,
) -> DataFrame:
    """
    Cria a fato de vendas da camada Gold.

    Grão:
    uma linha por item de pedido.
    """

    pedidos_base = pedidos_df.select(
        "pedido_id",
        "cliente_id",
        "vendedor_id",
        "data_pedido",
        "canal_venda",
        "status_pedido",
        "forma_pagamento",
    )

    itens_base = itens_df.select(
        "item_id",
        "pedido_id",
        "produto_id",
        "quantidade",
        "preco_unitario",
        "percentual_desconto",
        "valor_total_item",
        "valor_calculado",
    )

    cliente_keys = dim_cliente_df.select(
        "cliente_id",
        "cliente_sk",
    )

    produto_keys = dim_produto_df.select(
        "produto_id",
        "produto_sk",
    )

    vendedor_keys = dim_vendedor_df.select(
        "vendedor_id",
        "vendedor_sk",
    )

    fact_df = (
        itens_base
        .join(
            pedidos_base,
            "pedido_id",
            "inner",
        )
        .join(
            cliente_keys,
            "cliente_id",
            "inner",
        )
        .join(
            produto_keys,
            "produto_id",
            "inner",
        )
        .join(
            vendedor_keys,
            "vendedor_id",
            "inner",
        )
        .withColumn(
            "data_sk",
            date_format(
                col("data_pedido"),
                "yyyyMMdd",
            ).cast("long"),
        )
    )

    return fact_df.select(
        col("item_id").cast("long"),
        col("pedido_id").cast("long"),
        col("cliente_sk").cast("long"),
        col("produto_sk").cast("long"),
        col("vendedor_sk").cast("long"),
        col("data_sk").cast("long"),
        "canal_venda",
        "status_pedido",
        "forma_pagamento",
        col("quantidade").cast("integer"),
        col("preco_unitario").cast("double"),
        col("percentual_desconto").cast("double"),
        col("valor_total_item").cast("double"),
        col("valor_calculado").cast("double"),
    )