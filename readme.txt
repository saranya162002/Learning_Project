ecommerce-data-platform/
│
├── README.md
├── README-METADATA.md
├── README-EVENTS.md
├── INSTALLATION.md
├── ARCHITECTURE.md
├── CONTRIBUTING.md
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
│
├── docs/
│   ├── architecture/
│   │   ├── metadata-pipeline.drawio
│   │   ├── events-pipeline.drawio
│   │   └── ai-platform.drawio
│   │
│   ├── images/
│   │   ├── metadata-architecture.png
│   │   ├── events-architecture.png
│   │   └── overall-platform.png
│   │
│   └── decisions/
│       ├── why-delta-lake.md
│       ├── why-clickhouse.md
│       ├── why-opensearch.md
│       └── future-iceberg-roadmap.md
│
├── docker/
│   │
│   ├── spark/
│   │   └── Dockerfile
│   │
│   ├── airflow/
│   │   └── Dockerfile
│   │
│   ├── mcp/
│   │   └── Dockerfile
│   │
│   ├── api/
│   │   └── Dockerfile
│   │
│   └── dbt/
│       └── Dockerfile
│
├── cicd/
│   │
│   ├── github-actions/
│   │   ├── build.yml
│   │   ├── test.yml
│   │   └── release.yml
│   │
│   └── registry/
│       └── image-versioning.md
│
kubernetes/
│
├── kafka/
│   ├── deployment.yaml
│   ├── service.yaml
│   └── pvc.yaml
│
├── spark/
│   ├── spark-job.yaml
│   └── spark-configmap.yaml
│
├── airflow/
│   ├── deployment.yaml
│   └── service.yaml
│
├── clickhouse/
│   ├── deployment.yaml
│   └── pvc.yaml
│
├── opensearch/
│   ├── deployment.yaml
│   └── service.yaml
│
├── qdrant/
│   ├── deployment.yaml
│   └── pvc.yaml
│
├── minio/
│   ├── deployment.yaml
│   └── pvc.yaml
│
├── api/
│   ├── deployment.yaml
│   └── service.yaml
│
├── mcp/
│   ├── deployment.yaml
│   └── service.yaml
│
└── namespaces/    
│    └── ecommerce-platform.yaml
├──monitoring/
│   ├── grafana/
│   ├── prometheus/
│    ├── loki/
│    └── dashboards/
│
│
│
│
├── data/
│   │
│   ├── metadata/
│   │   ├── products.csv
│   │   ├── categories.csv
│   │   ├── brands.csv
│   │   └── sellers.csv
│   │
│   └── events/
│       ├── inventory_events.json
│       ├── click_events.json
│       ├── search_events.json
│       └── purchase_events.json
│
├── pipelines/
│   │
│   ├── metadata/
│   │   ├── ingestion/
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
│   │
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── checkpoints/
│
├── dbt/
│   │
│   ├── models/
│   │   ├── metadata/
│   │   └── events/
│   │
│   ├── tests/
│   ├── macros/
│   ├── seeds/
│   └── dbt_project.yml
│
├── analytics/
│   │
│   ├── clickhouse/
│   └── dashboards/
│
├── search/
│   │
│   ├── opensearch/
│   └── qdrant/
│
├── ai/
│   │
│   ├── rag/
│   ├── agents/
│   ├── embeddings/
│   └── prompts/
│
├── mcp/
│   │
│   ├── kafka_server.py
│   ├── clickhouse_server.py
│   ├── opensearch_server.py
│   ├── qdrant_server.py
│   ├── delta_server.py
│   ├── dbt_server.py
│   └── airflow_server.py
│
├── api/
│   │
│   ├── app.py
│   ├── routers/
│   └── services/
│
├── tests/
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