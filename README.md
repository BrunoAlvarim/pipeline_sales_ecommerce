# Pipeline Sales E-commerce

Pipeline de dados para processamento de vendas de e-commerce utilizando a arquitetura **Medallion** (Bronze → Silver → Gold) com **PySpark** e **Delta Lake**.

## Descrição

Este projeto implementa um pipeline ETL completo que extrai dados de uma API de e-commerce ([DummyJSON](https://dummyjson.com/)), transforma e carrega os dados em um Data Lakehouse utilizando o formato Delta Lake.

## Arquitetura

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│     BRONZE      │     │     SILVER      │     │      GOLD       │
│  (Raw Data)     │────▶│  (Cleaned)      │────▶│  (Aggregated)   │
│                 │     │                 │     │                 │
│ • carts         │     │ • dim_client    │     │ • sales_by_     │
│ • products      │     │ • dim_product   │     │   client        │
│ • users         │     │ • ft_sale       │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Estrutura do Projeto

```
pipeline_sales_ecommerce/
├── bronze/                     # Camada de extração (raw data)
│   ├── extract_carts.py        # Extração de carrinhos
│   ├── extract_products.py     # Extração de produtos
│   └── extract_user.py         # Extração de usuários
├── silver/                     # Camada de transformação
│   ├── transform_carts.py      # Transformação de carrinhos → ft_sale
│   ├── transform_products.py   # Transformação de produtos → dim_product
│   ├── transform_users.py      # Transformação de usuários → dim_client
│   └── func/
│       ├── clear_string.py     # Funções de limpeza de strings
│       └── schema_silver.py    # Schemas PySpark para parsing JSON
├── gold/                       # Camada de agregação (analytics)
│   └── agrup_sales_by_client.py # Agregação de vendas por cliente
├── func/
│   └── get_logging.py          # Configuração de logging
├── .gitignore
└── README.md
```

## Fluxo de Dados

### Bronze Layer (Ingestão)
- **Fonte**: API REST [DummyJSON](https://dummyjson.com/)
- **Endpoints**: `/users`, `/products`, `/carts`
- **Processo**: 
  - Extração paginada com `httpx`
  - Armazenamento do JSON bruto
  - Metadados: `ingestion_timestamp`, `row_id`, `source`
- **Destino**: Tabelas Delta `ecommerce.bronze.*`

### Silver Layer (Transformação)
- **Processo**:
  - Parsing de JSON com schemas tipados
  - Limpeza e padronização de strings
  - Deduplicação por window functions
  - Merge (upsert) com Delta Lake
- **Tabelas**:
  - `dim_client` - Dimensão de clientes
  - `dim_product` - Dimensão de produtos
  - `ft_sale` - Fato de vendas (carrinhos explodidos)

### Gold Layer (Agregação)
- **Processo**:
  - Join entre fato e dimensões
  - Agregações de negócio
  - Validação de qualidade de dados
- **Tabelas**:
  - `sales_by_client` - Total de vendas por cliente

## Tecnologias

| Tecnologia | Uso |
|------------|-----|
| **PySpark** | Processamento distribuído |
| **Databricks** | Orquestramento Pipeline |
| **Delta Lake** | Formato de armazenamento ACID |
| **httpx** | Cliente HTTP assíncrono |
| **pandas** | Conversão inicial de dados |

## Requisitos

- Apache Spark 3.x
- Delta Lake 2.x
- Python 3.8+
- Bibliotecas: `httpx`, `pandas`

## Execução

Os scripts foram desenvolvidos para execução em ambiente **Databricks** ou **Spark standalone**:

```python
# Exemplo de execução no Databricks
%run ./bronze/extract_user
%run ./bronze/extract_products
%run ./bronze/extract_carts

%run ./silver/transform_users
%run ./silver/transform_products
%run ./silver/transform_carts

%run ./gold/agrup_sales_by_client
```
## Licença

Este projeto é para fins educacionais.
