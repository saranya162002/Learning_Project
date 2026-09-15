# Medallion Design - Bronze Silver Gold

## Bronze Layer

Purpose:
- Raw ingestion
- Replayability
- Auditability

Tables:
- bronze.products
- bronze.categories
- bronze.brands
- bronze.sellers
- bronze.suppliers
- bronze.warehouses
- bronze.pricing
- bronze.inventory_updates
- bronze.price_updates
- bronze.seller_updates
- bronze.product_status_updates

Partition:
- ingestion_date

---

## Silver Layer

### silver.product_master
Join:
- products
- brands
- categories
- attributes
- images

### silver.inventory_current
Latest inventory by:
- sku_id
- warehouse_id

### silver.pricing_history
SCD Type 2

Columns:
- sku_id
- price
- effective_from
- effective_to
- is_current

### silver.seller_current
Current seller snapshot

### silver.product_360
Join:
- product_master
- inventory_current
- pricing_history
- seller_current

Business Entity Layer

---

## Gold Layer

### Dimensions

dim_product
- product_key
- sku_id
- product_name
- brand
- category

dim_brand
- brand_key
- brand_name

dim_seller
- seller_key
- seller_name
- rating

dim_warehouse
- warehouse_key
- city
- state

---

### Facts

fact_inventory
Grain:
- sku
- warehouse
- day

Measures:
- inventory_qty

fact_product_availability
Grain:
- sku
- day

Measures:
- inventory
- seller_count
- current_price

fact_price_history
Grain:
- sku
- price_version

Measures:
- price
- effective_from
- effective_to

---

## Processing Roadmap

1. Batch CSV → Bronze Delta
2. Kafka Streams → Bronze Delta
3. Product Master → Silver
4. Inventory Current → Silver
5. Pricing History (SCD2) → Silver
6. Product 360 → Silver
7. Dimensions → Gold
8. Facts → Gold
9. ClickHouse Serving
10. OpenSearch Indexing
11. Qdrant Embeddings

## Target Flow

Batch/Streams
    ↓
Kafka
    ↓
Bronze
    ↓
Silver
    ↓
Gold
    ↓
ClickHouse / OpenSearch / Qdrant
