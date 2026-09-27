import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker("pt_BR")
random.seed(42)
Faker.seed(42)

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

NUM_CLIENTES = 10_000
NUM_PRODUTOS = 20
NUM_VENDEDORES = 50
NUM_PEDIDOS = 100_000
DATA_INICIO = datetime(2025, 1, 1)
DATA_FIM = datetime(2026, 8, 1)


def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def generate_clientes():
    tipos = ["SUPERMERCADO", "DISTRIBUIDOR", "RESTAURANTE", "MERCADO", "EMPRESA"]
    rows = []
    for cliente_id in range(1, NUM_CLIENTES + 1):
        rows.append(
            {
                "cliente_id": cliente_id,
                "nome_cliente": fake.company(),
                "tipo_cliente": random.choice(tipos),
                "documento": fake.cnpj(),
                "cidade": fake.city(),
                "estado": fake.estado_sigla(),
                "data_cadastro": random_date(DATA_INICIO, DATA_FIM).date().isoformat(),
                "status_cliente": random.choice(["ATIVO", "ATIVO", "ATIVO", "INATIVO"]),
            }
        )
    df = pd.DataFrame(rows)
    bad = df.sample(frac=0.01, random_state=42).index
    df.loc[bad, "cidade"] = " MANAUS "
    return df


def generate_produtos():
    produtos_base = [
        ("Água Mineral 350ml", 350),
        ("Água Mineral 500ml", 500),
        ("Água Mineral 1L", 1000),
        ("Água Mineral 1.5L", 1500),
        ("Água Mineral 2L", 2000),
        ("Água Mineral 5L", 5000),
        ("Água Mineral 10L", 10000),
        ("Água Mineral 20L", 20000),
    ]
    rows = []
    for produto_id in range(1, NUM_PRODUTOS + 1):
        descricao, volume = random.choice(produtos_base)
        preco = round(random.uniform(2, 30), 2)
        rows.append(
            {
                "produto_id": produto_id,
                "descricao": descricao,
                "categoria": "AGUA_MINERAL",
                "volume_ml": volume,
                "tipo_embalagem": "GARRAFAO" if volume >= 10000 else "PET",
                "preco_unitario": preco,
                "custo_unitario": round(preco * random.uniform(0.45, 0.75), 2),
                "status_produto": "ATIVO",
            }
        )
    return pd.DataFrame(rows)


def generate_vendedores():
    rows = []
    for vendedor_id in range(1, NUM_VENDEDORES + 1):
        rows.append(
            {
                "vendedor_id": vendedor_id,
                "nome_vendedor": fake.name(),
                "cidade_base": fake.city(),
                "estado": fake.estado_sigla(),
                "data_admissao": random_date(datetime(2018, 1, 1), DATA_FIM).date().isoformat(),
                "status_vendedor": random.choice(["ATIVO", "ATIVO", "ATIVO", "INATIVO"]),
            }
        )
    return pd.DataFrame(rows)


def generate_pedidos():
    status = ["FATURADO", "ENTREGUE", "ENTREGUE", "ENTREGUE", "CANCELADO", "PENDENTE"]
    canais = ["VENDEDOR", "TELEFONE", "WHATSAPP", "PORTAL", "DISTRIBUIDOR"]
    pagamentos = ["PIX", "BOLETO", "CARTAO", "TRANSFERENCIA"]
    rows = []
    for pedido_id in range(1, NUM_PEDIDOS + 1):
        bruto = round(random.uniform(50, 5000), 2)
        desconto = round(bruto * random.uniform(0, 0.15), 2)
        rows.append(
            {
                "pedido_id": pedido_id,
                "cliente_id": random.randint(1, NUM_CLIENTES),
                "vendedor_id": random.randint(1, NUM_VENDEDORES),
                "data_pedido": random_date(DATA_INICIO, DATA_FIM).date().isoformat(),
                "canal_venda": random.choice(canais),
                "status_pedido": random.choice(status),
                "forma_pagamento": random.choice(pagamentos),
                "valor_bruto": bruto,
                "valor_desconto": desconto,
                "valor_total": round(bruto - desconto, 2),
            }
        )
    df = pd.DataFrame(rows)
    neg = df.sample(frac=0.002, random_state=1).index
    df.loc[neg, "valor_total"] *= -1
    nulls = df.sample(frac=0.002, random_state=2).index
    df.loc[nulls, "cliente_id"] = None
    return pd.concat([df, df.sample(100, random_state=3)], ignore_index=True)


def generate_itens(pedidos, produtos):
    preco_por_produto = dict(zip(produtos.produto_id, produtos.preco_unitario))
    rows, item_id = [], 1
    for pedido_id in pedidos.pedido_id.drop_duplicates():
        for produto_id in random.sample(list(preco_por_produto), random.randint(1, 5)):
            quantidade = random.randint(1, 50)
            preco = float(preco_por_produto[produto_id])
            perc_desc = round(random.uniform(0, 10), 2)
            total = round(quantidade * preco * (1 - perc_desc / 100), 2)
            rows.append(
                {
                    "item_id": item_id,
                    "pedido_id": int(pedido_id),
                    "produto_id": produto_id,
                    "quantidade": quantidade,
                    "preco_unitario": preco,
                    "percentual_desconto": perc_desc,
                    "valor_total_item": total,
                }
            )
            item_id += 1
    return pd.DataFrame(rows)


def generate_producao():
    rows = []
    for producao_id in range(1, 100_001):
        qtd = random.randint(500, 20_000)
        rows.append(
            {
                "producao_id": producao_id,
                "produto_id": random.randint(1, NUM_PRODUTOS),
                "data_producao": random_date(DATA_INICIO, DATA_FIM).date().isoformat(),
                "lote": f"LT{producao_id:08}",
                "turno": random.choice(["MANHA", "TARDE", "NOITE"]),
                "quantidade_produzida": qtd,
                "quantidade_descartada": random.randint(0, int(qtd * 0.03)),
                "linha_producao": random.choice(["LINHA_01", "LINHA_02", "LINHA_03"]),
            }
        )
    return pd.DataFrame(rows)


def generate_estoque():
    rows = []
    for estoque_id in range(1, 50_001):
        minimo = random.randint(100, 1000)
        rows.append(
            {
                "estoque_id": estoque_id,
                "produto_id": random.randint(1, NUM_PRODUTOS),
                "data_referencia": random_date(DATA_INICIO, DATA_FIM).date().isoformat(),
                "quantidade_estoque": random.randint(0, 10_000),
                "estoque_minimo": minimo,
                "estoque_maximo": minimo * random.randint(5, 12),
                "local_estoque": random.choice(["CD_MANAUS", "FABRICA", "EXPEDICAO"]),
            }
        )
    return pd.DataFrame(rows)


def generate_distribuicao(pedidos):
    rows = []
    base = pedidos.drop_duplicates("pedido_id")
    for i, row in base.iterrows():
        pedido_id = int(row.pedido_id)
        data_pedido = datetime.fromisoformat(row.data_pedido)
        saida = data_pedido + timedelta(days=random.randint(0, 2))
        prevista = saida + timedelta(days=random.randint(1, 5))
        entrega = prevista + timedelta(days=random.choice([0, 0, 0, 0, 1, 2, 3]))
        rows.append(
            {
                "entrega_id": pedido_id,
                "pedido_id": pedido_id,
                "data_saida": saida.date().isoformat(),
                "data_prevista": prevista.date().isoformat(),
                "data_entrega": entrega.date().isoformat(),
                "transportadora": random.choice(
                    ["TRANSPORTADORA_A", "TRANSPORTADORA_B", "FROTA_PROPRIA"]
                ),
                "rota": random.choice(
                    ["ROTA_NORTE", "ROTA_CENTRO", "ROTA_LESTE", "ROTA_OESTE", "ROTA_SUL"]
                ),
            }
        )
    return pd.DataFrame(rows)


def reconcile_pedidos_with_itens(pedidos, itens):
    """Align order financial totals to item totals, then inject a small set of
    controlled inconsistencies so the Silver quality rules have realistic failures.
    """
    base = pedidos.drop_duplicates("pedido_id", keep="first").copy()
    totals = (
        itens.groupby("pedido_id", as_index=False)["valor_total_item"]
        .sum()
        .rename(columns={"valor_total_item": "itens_total"})
    )
    base = base.merge(totals, on="pedido_id", how="left")
    base["valor_total"] = base["itens_total"].round(2)
    base["valor_desconto"] = (base["valor_bruto"] - base["valor_total"]).round(2)

    # Keep financial fields realistic: the gross amount is at least the item total.
    below = base["valor_bruto"] < base["valor_total"]
    base.loc[below, "valor_bruto"] = (base.loc[below, "valor_total"] * 1.05).round(2)
    base["valor_desconto"] = (base["valor_bruto"] - base["valor_total"]).round(2)

    # Controlled quality defects (small percentages).
    neg_idx = base.sample(frac=0.002, random_state=11).index
    base.loc[neg_idx, "valor_total"] *= -1
    null_idx = base.sample(frac=0.002, random_state=12).index
    base.loc[null_idx, "cliente_id"] = None
    inconsistent_idx = base.sample(frac=0.002, random_state=13).index
    base.loc[inconsistent_idx, "valor_total"] = (
        base.loc[inconsistent_idx, "valor_total"] + 7.35
    ).round(2)

    base = base.drop(columns=["itens_total"])
    duplicates = base.sample(100, random_state=14)
    return pd.concat([base, duplicates], ignore_index=True)


def main():
    clientes = generate_clientes()
    produtos = generate_produtos()
    vendedores = generate_vendedores()
    pedidos_raw = generate_pedidos()
    # Remove the defects generated in the raw helper; they are injected after item reconciliation.
    pedidos_seed = pedidos_raw.drop_duplicates("pedido_id", keep="first").copy()
    pedidos_seed["cliente_id"] = pedidos_seed["cliente_id"].fillna(1)
    pedidos_seed["valor_total"] = pedidos_seed["valor_total"].abs()
    itens = generate_itens(pedidos_seed, produtos)
    pedidos = reconcile_pedidos_with_itens(pedidos_seed, itens)
    producao = generate_producao()
    estoque = generate_estoque()
    distribuicao = generate_distribuicao(pedidos)

    datasets = {
        "clientes": clientes,
        "produtos": produtos,
        "vendedores": vendedores,
        "pedidos": pedidos,
        "itens_pedido": itens,
        "producao": producao,
        "estoque": estoque,
        "distribuicao": distribuicao,
    }

    for name, df in datasets.items():
        df.to_csv(DATA_DIR / f"{name}.csv", index=False)
        df.head(20).to_csv(DATA_DIR / "sample" / f"{name}_sample.csv", index=False)
        print(f"{name}: {len(df):,} registros")


if __name__ == "__main__":
    main()
