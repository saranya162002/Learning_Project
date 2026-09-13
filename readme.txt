ecommerce-data-platform/
│
├── README.md
├── README-METADATA.md
├── README-EVENTS.md
├── INSTALLATION.md
├── ARCHITECTURE.md
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
│
├── data/
│   ├── products.csv
│   ├── categories.csv
│   ├── brands.csv
│   ├── sellers.csv
│   └── inventory_events.json
│
├── infrastructure/
│   ├── kafka/
│   ├── minio/
│   ├── clickhouse/
│   ├── opensearch/
│   ├── qdrant/
│   └── airflow/
│
├── pipelines/
│   │
│   ├── metadata/
│   │   │
│   │   ├── ingestion/
│   │   │   ├── airbyte_loader.py
│   │   │   └── inventory_stream.py
│   │   │
│   │   ├── bronze/
│   │   ├── silver/
│   │   ├── gold/
│   │   └── serving/
│   │
│   └── events/
│       ├── ingestion/
│       ├── bronze/
│       ├── silver/
│       ├── gold/
│       └── serving/
│
├── lakehouse/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── checkpoints/
│
├── dbt/
│   ├── models/
│   │   ├── metadata/
│   │   └── events/
│   │
│   ├── tests/
│   ├── macros/
│   └── dbt_project.yml
│
├── analytics/
│   └── clickhouse/
│
├── search/
│   ├── opensearch/
│   └── qdrant/
│
├── ai/
│   ├── rag/
│   ├── agents/
│   └── prompts/
│
├── mcp/
│   ├── clickhouse_server.py
│   ├── kafka_server.py
│   ├── qdrant_server.py
│   ├── opensearch_server.py
│   └── delta_server.py
│
└── tests/