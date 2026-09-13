# E-Commerce Metadata Platform

## Overview

This pipeline manages all catalog and inventory-related data for the e-commerce platform.

The objective is to build a near real-time product catalog system capable of:

* Product catalog management
* Inventory management
* Pricing management
* Search catalog generation
* AI-powered semantic product discovery

---

## Data Sources

### Batch Sources

* products.csv
* categories.csv
* brands.csv
* sellers.csv

### CDC Sources

PostgreSQL Tables:

* products
* inventory
* pricing
* sellers
* warehouses

### Streaming Sources

Inventory Events

Examples:

* inventory_added
* inventory_removed
* inventory_adjustment
* stock_replenishment

---

## Architecture

```text
Batch Files
    │
    ▼
 Airbyte
    │

PostgreSQL
    │
Debezium CDC
    │

Inventory Events
    │
Kafka Producer
    │

    ┌──────────────┐
    │    Kafka     │
    └──────┬───────┘
           │
           ▼

Spark Structured Streaming

           │
           ▼

────────────────────────────
DELTA LAKE
────────────────────────────

Bronze
├── products_raw
├── inventory_raw
├── pricing_raw
└── sellers_raw

Silver
├── products
├── inventory
├── pricing
└── sellers

Gold
├── product_catalog
├── inventory_health
├── pricing_analytics
└── search_catalog

           │
           ▼

Serving Layer

├── ClickHouse
├── OpenSearch
└── Qdrant
```

---

## Bronze Layer

Raw immutable source data.

Tables:

* bronze.products_raw
* bronze.inventory_raw
* bronze.pricing_raw
* bronze.sellers_raw

---

## Silver Layer

Cleaned and conformed data.

Tables:

* silver.products
* silver.inventory
* silver.pricing
* silver.sellers

Transformations:

* schema standardization
* deduplication
* CDC merge logic
* null handling

---

## Gold Layer

Business-ready datasets.

### product_catalog

Unified product catalog.

### inventory_health

Inventory KPIs:

* stock coverage
* low stock products
* out-of-stock products

### pricing_analytics

Pricing trends.

### search_catalog

Served to OpenSearch.

---

## OLTP

PostgreSQL

Tables:

* products
* inventory
* pricing
* sellers

---

## OLAP

ClickHouse

Tables:

* product_catalog
* inventory_metrics
* pricing_metrics

---

## Search

OpenSearch

Indexes:

* products
* categories
* brands

---

## AI Components

Qdrant

Embeddings:

* product title
* description
* category
* brand

Use Cases:

* semantic search
* recommendations
* product discovery

---

## MCP Tools

* PostgreSQL MCP
* Kafka MCP
* Delta MCP
* dbt MCP
* ClickHouse MCP
* OpenSearch MCP
* Qdrant MCP
* Airflow MCP
* OpenMetadata MCP
