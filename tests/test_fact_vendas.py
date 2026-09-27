from datetime import date

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.transformation.fact_vendas import create_fact_vendas

PEDIDOS_SCHEMA = StructType(
    [
        StructField("pedido_id", LongType(), False),
        StructField("cliente_id", LongType(), False),
        StructField("vendedor_id", LongType(), False),
        StructField("data_pedido", DateType(), False),
        StructField("canal_venda", StringType(), True),
        StructField("status_pedido", StringType(), True),
        StructField("forma_pagamento", StringType(), True),
    ]
)

ITENS_SCHEMA = StructType(
    [
        StructField("item_id", LongType(), False),
        StructField("pedido_id", LongType(), False),
        StructField("produto_id", LongType(), False),
        StructField("quantidade", IntegerType(), False),
        StructField("preco_unitario", DoubleType(), False),
        StructField("percentual_desconto", DoubleType(), False),
        StructField("valor_total_item", DoubleType(), False),
        StructField("valor_calculado", DoubleType(), False),
    ]
)

DIM_CLIENTE_SCHEMA = StructType(
    [
        StructField("cliente_id", LongType(), False),
        StructField("cliente_sk", LongType(), False),
    ]
)

DIM_PRODUTO_SCHEMA = StructType(
    [
        StructField("produto_id", LongType(), False),
        StructField("produto_sk", LongType(), False),
    ]
)

DIM_VENDEDOR_SCHEMA = StructType(
    [
        StructField("vendedor_id", LongType(), False),
        StructField("vendedor_sk", LongType(), False),
    ]
)


def criar_dados_base(spark):
    pedidos = spark.createDataFrame(
        [
            (
                100,
                10,
                20,
                date(2026, 9, 26),
                "ONLINE",
                "FATURADO",
                "PIX",
            )
        ],
        schema=PEDIDOS_SCHEMA,
    )

    itens = spark.createDataFrame(
        [
            (
                1,
                100,
                30,
                2,
                10.0,
                10.0,
                18.0,
                18.0,
            )
        ],
        schema=ITENS_SCHEMA,
    )

    dim_cliente = spark.createDataFrame(
        [(10, 1000)],
        schema=DIM_CLIENTE_SCHEMA,
    )

    dim_produto = spark.createDataFrame(
        [(30, 3000)],
        schema=DIM_PRODUTO_SCHEMA,
    )

    dim_vendedor = spark.createDataFrame(
        [(20, 2000)],
        schema=DIM_VENDEDOR_SCHEMA,
    )

    return (
        pedidos,
        itens,
        dim_cliente,
        dim_produto,
        dim_vendedor,
    )


def test_criacao_fact_vendas(spark):
    dados = criar_dados_base(spark)

    result = create_fact_vendas(*dados).collect()[0]

    assert result["item_id"] == 1
    assert result["pedido_id"] == 100

    assert result["cliente_sk"] == 1000
    assert result["produto_sk"] == 3000
    assert result["vendedor_sk"] == 2000

    assert result["data_sk"] == 20260926

    assert result["canal_venda"] == "ONLINE"
    assert result["status_pedido"] == "FATURADO"
    assert result["forma_pagamento"] == "PIX"

    assert result["quantidade"] == 2
    assert result["preco_unitario"] == 10.0
    assert result["percentual_desconto"] == 10.0
    assert result["valor_total_item"] == 18.0
    assert result["valor_calculado"] == 18.0


def test_fact_vendas_preserva_grao_por_item(spark):
    (
        pedidos,
        _,
        dim_cliente,
        dim_produto,
        dim_vendedor,
    ) = criar_dados_base(spark)

    itens = spark.createDataFrame(
        [
            (1, 100, 30, 2, 10.0, 10.0, 18.0, 18.0),
            (2, 100, 30, 1, 10.0, 0.0, 10.0, 10.0),
        ],
        schema=ITENS_SCHEMA,
    )

    result = create_fact_vendas(
        pedidos,
        itens,
        dim_cliente,
        dim_produto,
        dim_vendedor,
    )

    assert result.count() == 2

    item_ids = {
        row["item_id"]
        for row in result.select("item_id").collect()
    }

    assert item_ids == {1, 2}


def test_fact_vendas_remove_item_sem_dimensao(spark):
    (
        pedidos,
        itens,
        dim_cliente,
        dim_produto,
        _,
    ) = criar_dados_base(spark)

    dim_vendedor = spark.createDataFrame(
        [(999, 9999)],
        schema=DIM_VENDEDOR_SCHEMA,
    )

    result = create_fact_vendas(
        pedidos,
        itens,
        dim_cliente,
        dim_produto,
        dim_vendedor,
    )

    assert result.count() == 0


def test_fact_vendas_colunas(spark):
    dados = criar_dados_base(spark)

    result = create_fact_vendas(*dados)

    assert result.columns == [
        "item_id",
        "pedido_id",
        "cliente_sk",
        "produto_sk",
        "vendedor_sk",
        "data_sk",
        "canal_venda",
        "status_pedido",
        "forma_pagamento",
        "quantidade",
        "preco_unitario",
        "percentual_desconto",
        "valor_total_item",
        "valor_calculado",
    ]


def test_fact_vendas_tipos(spark):
    dados = criar_dados_base(spark)

    result = create_fact_vendas(*dados)

    schema = {
        field.name: field.dataType
        for field in result.schema.fields
    }

    assert isinstance(schema["item_id"], LongType)
    assert isinstance(schema["pedido_id"], LongType)
    assert isinstance(schema["cliente_sk"], LongType)
    assert isinstance(schema["produto_sk"], LongType)
    assert isinstance(schema["vendedor_sk"], LongType)
    assert isinstance(schema["data_sk"], LongType)

    assert isinstance(schema["quantidade"], IntegerType)

    assert isinstance(schema["preco_unitario"], DoubleType)
    assert isinstance(schema["percentual_desconto"], DoubleType)
    assert isinstance(schema["valor_total_item"], DoubleType)
    assert isinstance(schema["valor_calculado"], DoubleType)