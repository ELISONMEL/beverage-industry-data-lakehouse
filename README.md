# Beverage Industry Data Lakehouse

Projeto de portfólio de **Engenharia de Dados** que implementa um Data Lakehouse end-to-end inspirado em um cenário industrial do setor de bebidas.

A solução utiliza dados sintéticos relacionados entre si para simular ingestão, transformação, qualidade de dados, quarentena, auditoria e modelagem dimensional seguindo a arquitetura **Medallion (Bronze → Silver → Gold)**.

> Todos os dados utilizados neste projeto são sintéticos e foram criados exclusivamente para fins educacionais e de demonstração técnica.

---

## 🎯 Objetivo

Construir uma plataforma analítica capaz de demonstrar, na prática, conceitos utilizados em projetos modernos de Engenharia de Dados:

- ingestão de dados;
- arquitetura Medallion;
- processamento distribuído com PySpark;
- armazenamento em Delta Lake;
- Data Quality;
- Quarantine;
- auditoria e idempotência;
- integridade referencial;
- modelagem dimensional;
- testes automatizados;
- qualidade de código;
- integração contínua com GitHub Actions.

---

## 🏗️ Arquitetura

Fluxo conceitual da solução:

```text
Oracle / CSV / JSON
        │
        ▼
Azure Data Factory
        │
        ▼
ADLS Gen2
        │
        ▼
Azure Databricks / PySpark
        │
        ▼
Delta Lake
        │
        ├── Bronze
        │
        ├── Silver
        │     └── Quarantine
        │
        └── Gold
              │
              ▼
           Power BI
```

O desenvolvimento local reproduz as principais etapas de processamento utilizando **PySpark + Delta Lake**, permitindo validar a arquitetura antes da execução em ambiente cloud.

---

## 🧰 Tecnologias

- Python 3.12
- Apache Spark / PySpark
- Delta Lake
- Azure Databricks
- Azure Data Factory
- Azure Data Lake Storage Gen2
- pytest
- pytest-cov
- Ruff
- Git
- GitHub
- GitHub Actions
- Power BI

---

## 📊 Dados sintéticos

O projeto possui um gerador próprio de dados que cria datasets relacionados para representar diferentes áreas de uma indústria de bebidas.

| Dataset | Registros gerados |
|---|---:|
| Clientes | 10.000 |
| Produtos | 20 |
| Vendedores | 50 |
| Pedidos | 100.100 |
| Itens de pedido | 300.045 |
| Produção | 100.000 |
| Estoque | 50.000 |
| Distribuição | 100.000 |

O gerador também injeta **defeitos controlados** nos dados para permitir a validação real das regras de Data Quality.

Entre os cenários simulados estão:

- valores negativos;
- campos obrigatórios nulos;
- valores financeiros inconsistentes;
- registros duplicados;
- violações de integridade referencial.

---

## 🥉 Bronze

A camada Bronze preserva os dados de origem com mínima transformação.

Principais características:

- ingestão parametrizada;
- schema explícito;
- armazenamento em Delta Lake;
- metadados técnicos de ingestão;
- identificação do arquivo de origem;
- identificação de batch;
- auditoria de arquivos processados;
- hash SHA-256;
- controle de idempotência.

A idempotência impede que um arquivo já processado seja carregado novamente de forma acidental.

---

## 🥈 Silver

A camada Silver aplica limpeza, tipagem, validações e regras de negócio.

Registros inválidos são direcionados para **Quarantine**, permitindo rastrear problemas sem interromper todo o processamento.

### Resultado das validações

| Dataset | Bronze | Silver | Quarantine | Qualidade |
|---|---:|---:|---:|---:|
| Clientes | 10.000 | 10.000 | 0 | 100% |
| Produtos | 20 | 20 | 0 | 100% |
| Vendedores | 50 | 50 | 0 | 100% |
| Pedidos | 100.100 | 99.401 | 599 | 99,40% |
| Itens de pedido | 300.045 | 298.194 | 1.851 | 99,38% |
| Produção | 100.000 | 100.000 | 0 | 100% |
| Estoque | 50.000 | 50.000 | 0 | 100% |
| Distribuição | 100.000 | 99.401 | 599 | 99,40% |

O projeto também demonstra **propagação de Data Quality por integridade referencial**.

Por exemplo, pedidos rejeitados na camada Silver podem provocar a rejeição de registros dependentes em itens de pedido ou distribuição.

---

## 🥇 Gold

A camada Gold implementa um modelo dimensional destinado ao consumo analítico.

### Dimensões

- `dim_cliente`
- `dim_produto`
- `dim_vendedor`
- `dim_data`

### Fatos

- `fact_vendas`
- `fact_producao`
- `fact_estoque`
- `fact_entregas`

### Registros Gold

| Tabela | Registros |
|---|---:|
| dim_cliente | 10.000 |
| dim_produto | 20 |
| dim_vendedor | 50 |
| dim_data | 588 |
| fact_vendas | 298.194 |
| fact_producao | 100.000 |
| fact_estoque | 50.000 |
| fact_entregas | 99.401 |

As dimensões utilizam **surrogate keys**, enquanto as tabelas fato mantêm relacionamentos com as dimensões correspondentes.

A `dim_data` suporta análises temporais e atende aos períodos utilizados pelas tabelas fato.

---

## 🔎 Data Quality

As validações implementadas incluem diferentes categorias de regras.

### Qualidade estrutural

- tipos de dados;
- campos obrigatórios;
- schemas explícitos.

### Qualidade de negócio

- valores financeiros válidos;
- consistência entre valores calculados e informados;
- quantidades válidas;
- regras específicas de cada domínio.

### Integridade referencial

São verificadas relações entre entidades como:

```text
Cliente → Pedido
Vendedor → Pedido
Pedido → Item
Produto → Item
Produto → Produção
Produto → Estoque
Pedido → Distribuição
```

Registros que não atendem às regras podem ser direcionados para a camada de **Quarantine**.

---

## 🧪 Testes automatizados

O projeto possui uma suíte automatizada utilizando **pytest**.

Atualmente:

```text
124 tests passed
```

Os testes cobrem componentes como:

- transformações Silver;
- dimensões Gold;
- fatos Gold;
- schemas;
- métricas de qualidade;
- auditoria;
- idempotência;
- ingestão Bronze;
- logger;
- regras financeiras;
- integridade referencial.

Os módulos críticos de transformação possuem cobertura unitária dedicada.

---

## ✅ Qualidade de código

O projeto utiliza **Ruff** para análise estática e padronização do código Python.

Validação local utilizada:

```bash
ruff check .
python -m compileall src pipelines scripts tests
pytest
```

O estado atual do projeto foi validado com sucesso nesses três níveis.

---

## 🔄 Continuous Integration

O repositório possui pipeline de CI utilizando **GitHub Actions**.

A cada `push` ou `pull request` configurado no workflow, o ambiente executa:

```text
Checkout
   ↓
Python 3.12
   ↓
Java 17
   ↓
Instalação das dependências
   ↓
Ruff
   ↓
pytest
```

O primeiro pipeline executado no GitHub Actions foi concluído com sucesso.

---

## 📁 Estrutura do projeto

```text
beverage-industry-data-lakehouse/
│
├── .github/
│   └── workflows/
│
├── architecture/
│
├── data/
│   └── sample/
│
├── docs/
│
├── notebooks/
│   ├── 01_bronze/
│   ├── 02_silver/
│   └── 03_gold/
│
├── pipelines/
│
├── scripts/
│
├── src/
│   ├── audit/
│   ├── config/
│   ├── ingestion/
│   ├── quality/
│   ├── schemas/
│   ├── transformation/
│   └── utils/
│
├── tests/
│
├── pytest.ini
├── requirements.txt
├── ruff.toml
└── README.md
```

---

## 🚀 Como executar

### 1. Clone o repositório

```bash
git clone https://github.com/ELISONMEL/beverage-industry-data-lakehouse.git
cd beverage-industry-data-lakehouse
```

### 2. Crie o ambiente virtual

Linux / WSL:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Windows:

```powershell
python -m venv .venv
.venv\Scripts\activate
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

### 4. Gere os dados sintéticos

```bash
python scripts/generate_data.py
```

### 5. Execute os testes

```bash
pytest
```

### 6. Execute a análise de código

```bash
ruff check .
```

---

## 🗺️ Roadmap

Entre as próximas evoluções planejadas estão:

- evolução da execução em ambiente Azure;
- integração com Azure Data Factory;
- execução em Azure Databricks;
- armazenamento em ADLS Gen2;
- evolução da observabilidade e auditoria;
- criação da camada semântica;
- dashboards no Power BI;
- evolução da estratégia de cargas incrementais;
- documentação visual da arquitetura.

---

## 🔐 Dados e privacidade

Nenhum dado real, confidencial ou proprietário de qualquer empresa é utilizado neste projeto.

Todos os datasets foram gerados artificialmente para demonstrar técnicas de Engenharia de Dados de forma segura e reproduzível.

---

## 👨‍💻 Autor

**Elison P Melgueiro**

Projeto desenvolvido como parte de um portfólio profissional focado em:

**Engenharia de Dados • Azure • Databricks • PySpark • Delta Lake • Data Quality**
