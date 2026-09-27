from src.transformation.itens_pedido_silver import (
    apply_itens_quality,
    transform_itens_pedido,
)

COLUMNS = [
    "item_id", "pedido_id", "produto_id", "quantidade",
    "preco_unitario", "percentual_desconto", "valor_total_item"
]


def test_calculo_valor_item(spark):
    df = spark.createDataFrame([(1, 100, 10, 10, 5.0, 10.0, 45.0)], COLUMNS)
    result = transform_itens_pedido(df).collect()[0]
    assert result.valor_calculado == 45.0


def test_item_inconsistente(spark):
    df = spark.createDataFrame([(1, 100, 10, 10, 5.0, 10.0, 48.0)], COLUMNS)
    result = apply_itens_quality(transform_itens_pedido(df)).collect()[0]
    assert result["_dq_reason"] == "VALOR_ITEM_INCONSISTENTE"


def test_produto_inexistente(spark):
    produtos = spark.createDataFrame([(1,), (2,), (3,)], ["produto_id"])
    itens = spark.createDataFrame([(100, 999)], ["item_id", "produto_id"])
    invalidos = itens.join(produtos, "produto_id", "left_anti")
    assert invalidos.count() == 1
    assert invalidos.collect()[0].produto_id == 999
