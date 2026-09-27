from pyspark.sql.types import StringType

from src.schemas.clientes_schema import CLIENTES_SCHEMA
from src.schemas.distribuicao_schema import DISTRIBUICAO_SCHEMA
from src.schemas.estoque_schema import ESTOQUE_SCHEMA
from src.schemas.itens_pedido_schema import ITENS_PEDIDO_SCHEMA
from src.schemas.pedidos_schema import PEDIDOS_SCHEMA
from src.schemas.producao_schema import PRODUCAO_SCHEMA
from src.schemas.produtos_schema import PRODUTOS_SCHEMA
from src.schemas.vendedores_schema import VENDEDORES_SCHEMA

SCHEMAS_ESPERADOS = [
    (
        CLIENTES_SCHEMA,
        [
            "cliente_id",
            "nome_cliente",
            "tipo_cliente",
            "documento",
            "cidade",
            "estado",
            "data_cadastro",
            "status_cliente",
        ],
    ),
    (
        DISTRIBUICAO_SCHEMA,
        [
            "entrega_id",
            "pedido_id",
            "data_saida",
            "data_prevista",
            "data_entrega",
            "transportadora",
            "rota",
        ],
    ),
    (
        ESTOQUE_SCHEMA,
        [
            "estoque_id",
            "produto_id",
            "data_referencia",
            "quantidade_estoque",
            "estoque_minimo",
            "estoque_maximo",
            "local_estoque",
        ],
    ),
    (
        ITENS_PEDIDO_SCHEMA,
        [
            "item_id",
            "pedido_id",
            "produto_id",
            "quantidade",
            "preco_unitario",
            "percentual_desconto",
            "valor_total_item",
        ],
    ),
    (
        PEDIDOS_SCHEMA,
        [
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
        ],
    ),
    (
        PRODUCAO_SCHEMA,
        [
            "producao_id",
            "produto_id",
            "data_producao",
            "lote",
            "turno",
            "quantidade_produzida",
            "quantidade_descartada",
            "linha_producao",
        ],
    ),
    (
        PRODUTOS_SCHEMA,
        [
            "produto_id",
            "descricao",
            "categoria",
            "volume_ml",
            "tipo_embalagem",
            "preco_unitario",
            "custo_unitario",
            "status_produto",
        ],
    ),
    (
        VENDEDORES_SCHEMA,
        [
            "vendedor_id",
            "nome_vendedor",
            "cidade_base",
            "estado",
            "data_admissao",
            "status_vendedor",
        ],
    ),
]


def test_schemas_possuem_colunas_esperadas():
    for schema, expected_columns in SCHEMAS_ESPERADOS:
        assert schema.fieldNames() == expected_columns


def test_bronze_preserva_campos_como_string():
    for schema, _ in SCHEMAS_ESPERADOS:
        for field in schema.fields:
            assert isinstance(field.dataType, StringType)


def test_bronze_permite_valores_nulos():
    for schema, _ in SCHEMAS_ESPERADOS:
        for field in schema.fields:
            assert field.nullable is True