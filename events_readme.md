# E-Commerce Events Platform

## Overview

This platform captures and processes user behavioral events.

Objectives:

* Clickstream analytics
* Customer journey analysis
* Search analytics
* Recommendation features
* Real-time metrics

---

## Event Sources

Frontend SDK Events

* product_view
* product_click
* search
* add_to_cart
* remove_from_cart
* checkout
* purchase

---

## Architecture

```text
Frontend
Mobile App
Web App

      │
      ▼

Kafka Topics

├── views
├── clicks
├── searches
├── carts
└── purchases

      │
      ▼

Spark Structured Streaming

      │
      ▼

────────────────────────────
DELTA LAKE
────────────────────────────

Bronze
├── views_raw
├── clicks_raw
├── searches_raw
├── carts_raw
└── purchases_raw

Silver
├── user_sessions
├── product_interactions
├── search_events
└── customer_events

Gold
├── product_metrics
├── customer_360
├── search_analytics
├── conversion_funnel
└── recommendation_features

      │
      ▼

Serving Layer

├── ClickHouse
├── Qdrant
└── Feature Store
```

---

## Bronze Layer

Raw event data.

Tables:

* bronze.views_raw
* bronze.clicks_raw
* bronze.searches_raw
* bronze.carts_raw
* bronze.purchases_raw

Partitioning:

* event_date
* event_hour

---

## Silver Layer

Cleaned event streams.

### user_sessions

Sessionized customer activity.

### product_interactions

Customer-product interactions.

### search_events

Search behavior tracking.

### customer_events

Unified behavioral dataset.

---

## Gold Layer

### product_metrics

Daily product engagement.

### customer_360

Customer profile generation.

### search_analytics

Search performance metrics.

### conversion_funnel

View → Click → Cart → Purchase.

### recommendation_features

Feature generation for ML systems.

---

## OLAP

ClickHouse

Analytics:

* product trends
* customer trends
* search trends
* conversion metrics

---

## AI Components

Qdrant

Embeddings:

* search history
* click history
* purchase history
* customer journeys

Use Cases:

* recommendations
* personalization
* conversational analytics

---

## MCP Tools

* Kafka MCP
* Spark MCP
* Delta MCP
* dbt MCP
* ClickHouse MCP
* Qdrant MCP
* Airflow MCP
* OpenMetadata MCP

---

## Example Queries

Top searched products

Top converting products

Products viewed but not purchased

Customer churn indicators

Frequently co-viewed products
