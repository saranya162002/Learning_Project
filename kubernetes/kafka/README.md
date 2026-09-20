# Kafka + ZooKeeper on Kubernetes — Setup Guide

## Architecture Overview

```
                    ┌─────────────────────────────────────────────┐
                    │               ZooKeeper Pod                 │
                    │               zookeeper-0                   │
                    │                (5Gi PVC)                    │
                    └──────────────────────┬──────────────────────┘
                                           │ (Zookeeper Connection)
                    ┌──────────────────────┴──────────────────────┐
                    │          Kafka Cluster (2 Brokers)          │
                    │                                             │
  Batch Spark   ───▶│         kafka-0           kafka-1          │◀─── Stream Spark
  Connector         │        (5Gi PVC)         (5Gi PVC)          │     Connector
  (4 topics)        │                                             │     (2+ topics)
                    └──────────────────────┬──────────────────────┘
                                           │
                                    Airflow triggers
                                           │
                                           ▼
                                    Bronze Layer (11 tables)
                                     Delta Lake on MinIO
```

### Kafka Topics → Bronze Table Mapping (Even Partitions & Replication = 2)

| Topic | Source | Bronze Table | Partitions | Replication |
|-------|--------|--------------|------------|-------------|
| `ecommerce.batch.products` | Batch CSV Connector | `bronze.products` | **2** | 2 |
| `ecommerce.batch.categories` | Batch CSV Connector | `bronze.categories` | **2** | 2 |
| `ecommerce.batch.brands` | Batch CSV Connector | `bronze.brands` | **2** | 2 |
| `ecommerce.batch.sellers` | Batch CSV Connector | `bronze.sellers` | **2** | 2 |
| `ecommerce.batch.suppliers` | Batch CSV Connector | `bronze.suppliers` | **2** | 2 |
| `ecommerce.batch.warehouses` | Batch CSV Connector | `bronze.warehouses` | **2** | 2 |
| `ecommerce.batch.pricing` | Batch CSV Connector | `bronze.pricing` | **2** | 2 |
| `ecommerce.stream.inventory-updates` | Redis Stream Connector | `bronze.inventory_updates` | **6** | 2 |
| `ecommerce.stream.price-updates` | Redis Stream Connector | `bronze.price_updates` | **6** | 2 |
| `ecommerce.stream.seller-updates` | Redis Stream Connector | `bronze.seller_updates` | **4** | 2 |
| `ecommerce.stream.product-status-updates` | Redis Stream Connector | `bronze.product_status_updates` | **4** | 2 |

---

## Prerequisites

- **Kubernetes cluster** running (Minikube, Docker Desktop K8s, kind, or cloud)
- **kubectl** installed and configured
- **k9s** (optional, for monitoring)

---

## Files in This Directory

```
kubernetes/kafka/
├── namespace.yaml         # ecommerce-platform namespace
├── zookeeper.yaml         # ZooKeeper StatefulSet, Service, and PVC (5Gi)
├── configmap.yaml         # Kafka server.properties (ZooKeeper settings)
├── pvc.yaml               # Standalone PVC reference (5Gi)
├── deployment.yaml        # 2-broker Kafka StatefulSet (5Gi PVC each)
├── service.yaml           # Headless + ClusterIP services
├── topic-init-job.yaml    # Job to create all 11 topics (even partition counts)
└── README.md              # This file
```

---

## Step-by-Step Setup

### Step 1 — Create the Namespace

```bash
kubectl apply -f kubernetes/kafka/namespace.yaml
```

### Step 2 — Deploy ZooKeeper with PVC

```bash
kubectl apply -f kubernetes/kafka/zookeeper.yaml
```

Wait for ZooKeeper to be ready:

```bash
kubectl wait --for=condition=Ready pod zookeeper-0 -n ecommerce-platform --timeout=60s
```

### Step 3 — Apply the Kafka ConfigMap

```bash
kubectl apply -f kubernetes/kafka/configmap.yaml
```

### Step 4 — Deploy the 2-Broker Kafka StatefulSet

```bash
kubectl apply -f kubernetes/kafka/deployment.yaml
```

Wait for both brokers to become Ready:

```bash
kubectl get pods -n ecommerce-platform -l app.kubernetes.io/name=kafka -w
```

Expected output:

```
NAME      READY   STATUS    RESTARTS   AGE
kafka-0   1/1     Running   0          45s
kafka-1   1/1     Running   0          40s
```

### Step 5 — Create Services

```bash
kubectl apply -f kubernetes/kafka/service.yaml
```

### Step 6 — Create Topics (Even Partition Counts)

```bash
kubectl apply -f kubernetes/kafka/topic-init-job.yaml
```

Watch the job logs:

```bash
kubectl logs -f job/kafka-topic-init -n ecommerce-platform
```

Verify created topics and partitions:

```bash
kubectl exec -it kafka-0 -n ecommerce-platform -- \
  kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

## One-Command Deploy (All at Once)

```bash
# 1. Apply Namespace & ZooKeeper
kubectl apply -f kubernetes/kafka/namespace.yaml
kubectl apply -f kubernetes/kafka/zookeeper.yaml

# 2. Wait for ZooKeeper
kubectl wait --for=condition=Ready pod zookeeper-0 -n ecommerce-platform --timeout=90s

# 3. Apply Kafka Config, StatefulSet & Services
kubectl apply -f kubernetes/kafka/configmap.yaml
kubectl apply -f kubernetes/kafka/deployment.yaml
kubectl apply -f kubernetes/kafka/service.yaml

# 4. Wait for Kafka Brokers
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=kafka \
  -n ecommerce-platform --timeout=120s

# 5. Execute Topic Init Job
kubectl apply -f kubernetes/kafka/topic-init-job.yaml
```

---

## Verification & Operations

### Check PVCs & Storage

```bash
kubectl get pvc -n ecommerce-platform
```

Expected output:

```
NAME                         STATUS   VOLUME                                     CAPACITY   ACCESS MODES   AGE
kafka-data-kafka-0           Bound    pvc-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx   5Gi        RWO            2m
kafka-data-kafka-1           Bound    pvc-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx   5Gi        RWO            2m
zookeeper-data-zookeeper-0   Bound    pvc-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx   5Gi        RWO            3m
zookeeper-datalog-zookeeper-0 Bound   pvc-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx   5Gi        RWO            3m
```

### Inspect Topic Details & Partitions

```bash
kubectl exec -it kafka-0 -n ecommerce-platform -- \
  kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic ecommerce.batch.products
```

---

## Cleanup

```bash
# Delete topics job & workloads
kubectl delete job kafka-topic-init -n ecommerce-platform
kubectl delete statefulset kafka zookeeper -n ecommerce-platform
kubectl delete svc kafka kafka-headless zookeeper -n ecommerce-platform
kubectl delete configmap kafka-config -n ecommerce-platform

# Delete PVCs (CAUTION: deletes all stored Kafka & ZooKeeper data)
kubectl delete pvc -l app.kubernetes.io/name=kafka -n ecommerce-platform
kubectl delete pvc -l app.kubernetes.io/name=zookeeper -n ecommerce-platform
```
