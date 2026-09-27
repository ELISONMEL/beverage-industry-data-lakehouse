from src.transformation.pedidos_silver import apply_pedidos_quality, transform_pedidos

COLUMNS = [
    "pedido_id",
    "cliente_id",
    "vendedor_id",
    "data_pedido",
    "canal_venda",
    "status_pedido",
    "forma_pagamento",
    "valor_bruto",
    "valor_desconto",
    "valor_total",
]


def create_pedido_df(spark, bruto, desconto, total):
    return spark.createDataFrame(
        [
            (
                "1001",
                "10",
                "20",
                "2026-07-10",
                "VENDEDOR",
                "FATURADO",
                "PIX",
                str(bruto),
                str(desconto),
                str(total),
            )
        ],
        COLUMNS,
    )


def test_pedido_valido(spark):
    result = apply_pedidos_quality(
        transform_pedidos(create_pedido_df(spark, 100, 10, 90))
    ).collect()[0]
    assert result["_dq_reason"] is None


def test_valor_total_negativo(spark):
    result = apply_pedidos_quality(
        transform_pedidos(create_pedido_df(spark, 100, 10, -90))
    ).collect()[0]
    assert result["_dq_reason"] == "VALOR_TOTAL_NEGATIVO"


def test_total_pedido_inconsistente(spark):
    result = apply_pedidos_quality(
        transform_pedidos(create_pedido_df(spark, 100, 10, 95))
    ).collect()[0]
    assert result["_dq_reason"] == "VALOR_TOTAL_INCONSISTENTE"


def test_valor_total_nulo(spark):
    df = create_pedido_df(spark, 100, 10, None)
    result = apply_pedidos_quality(transform_pedidos(df)).collect()[0]
    assert result["_dq_reason"] == "VALOR_TOTAL_NULL"
