# Beverage Industry Data Lakehouse

Projeto de portfólio de Engenharia de Dados inspirado em um cenário industrial de bebidas. Todos os dados são sintéticos e criados exclusivamente para fins educacionais e de demonstração técnica.

## Objetivo
Construir uma plataforma analítica end-to-end com arquitetura Medallion, qualidade de dados, quarentena, auditoria, modelo dimensional e CI.

## Arquitetura
Oracle / CSV / JSON → Azure Data Factory → ADLS Gen2 → Databricks / PySpark → Delta Lake → Bronze → Silver / Quarantine → Gold → Power BI

## Principais tecnologias
- Python
- PySpark
- Delta Lake
- Azure Databricks
- Azure Data Factory
- ADLS Gen2
- GitHub Actions
- pytest
- Power BI

## Estrutura
- `scripts/`: geração dos dados sintéticos
- `src/ingestion/`: ingestão genérica para Bronze
- `src/transformation/`: regras Silver
- `src/quality/`: métricas e validações
- `tests/`: testes automatizados
- `notebooks/`: exemplos de orquestração Bronze/Silver/Gold
- `.github/workflows/`: CI

## Quick start
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
python scripts/generate_data.py
pytest
```

## Aviso
Nenhum dado real ou confidencial de qualquer empresa é utilizado neste repositório.


## Validação
- O gerador produz dados sintéticos relacionados entre si e injeta defeitos controlados para exercitar Data Quality.
- Os testes unitários cobrem padronização, regras financeiras, nulos e consistência de itens.
- O workflow de CI usa Python 3.11 e Java 17 para executar pytest em cada push/PR.
