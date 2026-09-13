# Installation Guide

## Prerequisites

### macOS

Recommended:

* macOS 14+
* Docker Desktop
* Homebrew
* Python 3.11
* Java 17

---

## Step 1: Install Homebrew

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

---

## Step 2: Install Java

```bash
brew install openjdk@17
```

Verify:

```bash
java -version
```

---

## Step 3: Install Docker Desktop

Install Docker Desktop and verify:

```bash
docker --version
docker compose version
```

---

## Step 4: Create Virtual Environment

```bash
python3 -m venv venv

source venv/bin/activate
```

---

## Step 5: Install Python Dependencies

```bash
pip install -r requirements.txt
```

---

## Step 6: Start Infrastructure

```bash
docker compose up -d
```

Expected services:

* Kafka
* PostgreSQL
* MinIO
* ClickHouse
* OpenSearch
* Qdrant
* Airflow

---

## Step 7: Verify Services

Kafka:

```bash
docker ps
```

ClickHouse:

```text
http://localhost:8123
```

Airflow:

```text
http://localhost:8080
```

MinIO:

```text
http://localhost:9001
```

OpenSearch:

```text
http://localhost:9200
```

Qdrant:

```text
http://localhost:6333
```

---

## Step 8: Create Delta Lake Storage

MinIO Buckets:

```text
bronze
silver
gold
```

---

## Step 9: Run Metadata Pipeline

```bash
python pipelines/metadata/metadata_stream.py
```

---

## Step 10: Run Events Pipeline

```bash
python pipelines/events/events_stream.py
```

---

## Step 11: Execute dbt Models

```bash
dbt seed

dbt run

dbt test
```

---

## Step 12: Generate Documentation

```bash
dbt docs generate

dbt docs serve
```

---

## Step 13: Load Analytics Tables

Metadata Gold → ClickHouse

Events Gold → ClickHouse

---

## Step 14: Build Vector Indexes

Generate embeddings.

Load:

* Product Catalog
* Search History
* Customer Journeys

Into:

Qdrant

---

## Step 15: Enable MCP Layer

Expose:

* PostgreSQL
* Kafka
* ClickHouse
* Delta Lake
* dbt
* OpenSearch
* Qdrant

Through MCP servers.

The platform is now ready for analytics, AI agents, semantic search, and natural-language querying.
