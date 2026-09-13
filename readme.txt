ecommerce-data-platform/
│
├── README.md
├── README-METADATA.md
├── README-EVENTS.md
├── INSTALLATION.md
├── ARCHITECTURE.md
├── docker-compose.yml
├── requirements.txt
├── .env.example
│
├── docs/
│   ├── architecture/
│   │   ├── metadata-pipeline.drawio
│   │   ├── event-pipeline.drawio
│   │   ├── lakehouse.drawio
│   │   └── ai-platform.drawio
│   │
│   ├── images/
│   │   ├── metadata-architecture.png
│   │   ├── events-architecture.png
│   │   └── end-to-end-architecture.png
│   │
│   └── decisions/
│       ├── delta-vs-iceberg.md
│       ├── why-clickhouse.md
│       └── why-opensearch.md
│
├── infrastructure/
│   │
│   ├── kafka/
│   │   ├── topics.yaml
│   │   └── kafka-init.sh
│   │
│   ├── postgres/
│   │   ├── init.sql
│   │   └── seed_data.sql
│   │
│   ├── minio/
│   │   └── create_buckets.sh
│   │
│   ├── clickhouse/
│   │   ├── ddl/
│   │   └── views/
│   │
│   ├── opensearch/
│   │   └── index_templates/
│   │
│   ├── qdrant/
│   │   └── collections/
│   │
│   └── airflow/
│       ├── dags/
│       └── plugins/
│
├── configs/
│   ├── spark/
│   │   ├── spark-defaults.conf
│   │   └── log4j.properties
│   │
│   ├── dbt/
│   │   └── profiles.yml
│   │
│   └── logging/
│       └── logging.yaml
│
├── data/
│   │
│   ├── sample/
│   │   ├── products.csv
│   │   ├── categories.csv
│   │   ├── sellers.csv
│   │   └── inventory.csv
│   │
│   └── generated/
│
├── pipelines/
│   │
│   ├── metadata/
│   │   │
│   │   ├── ingestion/
│   │   │   ├── airbyte_ingestion.py
│   │   │   ├── postgres_cdc.py
│   │   │   └── inventory_events.py
│   │   │
│   │   ├── bronze/
│   │   │   ├── products_bronze.py
│   │   │   ├── inventory_bronze.py
│   │   │   └── pricing_bronze.py
│   │   │
│   │   ├── silver/
│   │   │   ├── products_silver.py
│   │   │   ├── inventory_silver.py
│   │   │   └── pricing_silver.py
│   │   │
│   │   ├── gold/
│   │   │   ├── product_catalog.py
│   │   │   ├── inventory_health.py
│   │   │   └── pricing_analytics.py
│   │   │
│   │   └── serving/
│   │       ├── clickhouse_loader.py
│   │       ├── opensearch_loader.py
│   │       └── qdrant_loader.py
│   │
│   └── events/
│       │
│       ├── ingestion/
│       │   ├── click_events.py
│       │   ├── search_events.py
│       │   ├── cart_events.py
│       │   └── purchase_events.py
│       │
│       ├── bronze/
│       │   ├── clicks_bronze.py
│       │   ├── views_bronze.py
│       │   └── searches_bronze.py
│       │
│       ├── silver/
│       │   ├── sessionization.py
│       │   ├── search_events.py
│       │   └── customer_events.py
│       │
│       ├── gold/
│       │   ├── product_metrics.py
│       │   ├── customer_360.py
│       │   ├── conversion_funnel.py
│       │   └── search_analytics.py
│       │
│       └── serving/
│           ├── clickhouse_loader.py
│           └── qdrant_loader.py
│
├── lakehouse/
│   │
│   ├── delta/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   │
│   └── checkpoints/
│
├── dbt/
│   │
│   ├── models/
│   │   ├── metadata/
│   │   │   ├── product_catalog.sql
│   │   │   ├── inventory_health.sql
│   │   │   └── pricing_analytics.sql
│   │   │
│   │   └── events/
│   │       ├── customer_360.sql
│   │       ├── search_analytics.sql
│   │       └── conversion_funnel.sql
│   │
│   ├── tests/
│   │
│   ├── macros/
│   │
│   └── dbt_project.yml
│
├── analytics/
│   │
│   ├── clickhouse/
│   │   ├── product_metrics.sql
│   │   ├── search_metrics.sql
│   │   └── customer_metrics.sql
│   │
│   └── dashboards/
│       ├── inventory_dashboard.json
│       └── ecommerce_dashboard.json
│
├── search/
│   │
│   ├── opensearch/
│   │   ├── mappings/
│   │   └── ingest_pipelines/
│   │
│   └── qdrant/
│       ├── embeddings.py
│       ├── collections.py
│       └── loaders.py
│
├── ai/
│   │
│   ├── rag/
│   │   ├── retriever.py
│   │   └── vector_loader.py
│   │
│   ├── agents/
│   │   ├── analytics_agent.py
│   │   ├── catalog_agent.py
│   │   └── customer_agent.py
│   │
│   └── prompts/
│       ├── analytics.txt
│       └── recommendations.txt
│
├── mcp/
│   │
│   ├── clickhouse_server.py
│   ├── kafka_server.py
│   ├── postgres_server.py
│   ├── qdrant_server.py
│   └── opensearch_server.py
│
├── tests/
│   │
│   ├── metadata/
│   ├── events/
│   ├── dbt/
│   └── integration/
│
└── scripts/
    ├── start_platform.sh
    ├── stop_platform.sh
    ├── reset_environment.sh
    └── generate_sample_data.py