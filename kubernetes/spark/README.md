# Spark on Kubernetes — Setup & Airflow Orchestration Guide

## Architecture Overview

```
                      ┌──────────────────────────────────────────────┐
                      │          Airflow DAG Orchestration           │
                      │  (Triggers SparkKubernetesOperator / Submit) │
                      └──────────────────────┬───────────────────────┘
                                             │
                                             ▼
                      ┌──────────────────────────────────────────────┐
                      │            Spark Operator Controller          │
                      │     (Reconciles SparkApplication CRDs)       │
                      └──────────────────────┬───────────────────────┘
                                             │
                      ┌──────────────────────┴──────────────────────┐
                      │          Spark Driver Pod (1 Core)          │
                      │             ServiceAccount: spark            │
                      └──────────────────────┬──────────────────────┘
                                             │ (Dynamic Pod Allocation)
                      ┌──────────────────────┴──────────────────────┐
                      │                                             │
                      ▼                                             ▼
        ┌───────────────────────────┐                 ┌───────────────────────────┐
        │   Spark Executor Pod 1    │                 │   Spark Executor Pod 2    │
        │   (Delta Lake / Kafka)    │                 │   (Delta Lake / Kafka)    │
        └───────────────────────────┘                 └───────────────────────────┘
```

---

## Docker Image Updates

The custom Spark Dockerfile in `docker/spark/DockerFile` builds on `ecommerce-platform-base:latest`:

- **Spark Version**: Apache Spark `3.5.1` (with Hadoop 3)
- **Delta Lake**: `delta-spark==3.2.0` & `delta-storage-3.2.0.jar`
- **Kafka Integration**: `spark-sql-kafka-0-10_2.12-3.5.1.jar` & `kafka-clients-3.5.1.jar`
- **Environment**:
  - `SPARK_HOME=/opt/spark`
  - `PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH`
  - Entrypoint: `/opt/spark/kubernetes/dockerfiles/spark/entrypoint.sh`

---

## Files in This Directory

```
kubernetes/spark/
├── spark-operator-rbac.yaml              # RBAC Roles, ServiceAccounts (spark, spark-operator) & Bindings
├── spark-configmap.yaml                  # spark-defaults.conf (Delta catalog, Kafka, MinIO endpoint)
├── spark-operator.yaml                   # SparkApplication CRD & Spark Operator Controller
├── spark-job-bronze-ingestion.yaml       # SparkApplication CRD for Bronze Ingestion
├── spark-job-silver-transformation.yaml   # SparkApplication CRD for Silver Product 360
├── spark-job-gold-aggregation.yaml       # SparkApplication CRD for Gold Star Schema
└── README.md                             # Setup & Airflow Integration guide
```

---

## Step-by-Step Deployment

### Step 1 — Apply Namespace & RBAC

Ensure the `ecommerce-platform` namespace exists and grant RBAC permissions:

```bash
kubectl apply -f kubernetes/kafka/namespace.yaml
kubectl apply -f kubernetes/spark/spark-operator-rbac.yaml
```

### Step 2 — Apply Spark ConfigMap

Load default Spark configs (Delta catalog extensions, Kafka broker hostnames, MinIO credentials):

```bash
kubectl apply -f kubernetes/spark/spark-configmap.yaml
```

### Step 3 — Deploy Spark Operator & CRDs

Install the Spark Operator controller and the `SparkApplication` CustomResourceDefinition:

```bash
kubectl apply -f kubernetes/spark/spark-operator.yaml
```

Wait for the Spark Operator pod to reach `Running` state:

```bash
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=spark-operator \
  -n ecommerce-platform --timeout=60s
```

### Step 4 — Run Spark Jobs (Manual Trigger)

You can trigger Spark jobs manually via `kubectl apply`:

```bash
# 1. Trigger Bronze Ingestion Job
kubectl apply -f kubernetes/spark/spark-job-bronze-ingestion.yaml

# 2. Check SparkApplication status
kubectl get sparkapp -n ecommerce-platform

# 3. View Driver and Executor pods
kubectl get pods -n ecommerce-platform -l app.kubernetes.io/name=spark
```

---

## Airflow DAG Integration

Airflow orchestrates Spark jobs in Kubernetes using the `SparkKubernetesOperator` or `SparkSubmitOperator`.

### Example Airflow DAG (`medallion_pipeline_dag.py`)

Place this Python DAG in Airflow DAGs directory (`pipelines/airflow/dags/`):

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='medallion_spark_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['spark', 'medallion', 'bronze', 'silver', 'gold'],
) as dag:

    # 1. Bronze Ingestion Task
    bronze_task = SparkKubernetesOperator(
        task_id='spark_bronze_ingestion',
        namespace='ecommerce-platform',
        application_file='spark-job-bronze-ingestion.yaml',
        do_xcom_push=True,
    )

    bronze_sensor = SparkKubernetesSensor(
        task_id='spark_bronze_sensor',
        namespace='ecommerce-platform',
        application_name="{{ task_instance.xcom_pull(task_ids='spark_bronze_ingestion')['metadata']['name'] }}",
        attach_log=True,
    )

    # 2. Silver Transformation Task
    silver_task = SparkKubernetesOperator(
        task_id='spark_silver_transformation',
        namespace='ecommerce-platform',
        application_file='spark-job-silver-transformation.yaml',
        do_xcom_push=True,
    )

    silver_sensor = SparkKubernetesSensor(
        task_id='spark_silver_sensor',
        namespace='ecommerce-platform',
        application_name="{{ task_instance.xcom_pull(task_ids='spark_silver_transformation')['metadata']['name'] }}",
        attach_log=True,
    )

    # 3. Gold Aggregation Task
    gold_task = SparkKubernetesOperator(
        task_id='spark_gold_aggregation',
        namespace='ecommerce-platform',
        application_file='spark-job-gold-aggregation.yaml',
        do_xcom_push=True,
    )

    gold_sensor = SparkKubernetesSensor(
        task_id='spark_gold_sensor',
        namespace='ecommerce-platform',
        application_name="{{ task_instance.xcom_pull(task_ids='spark_gold_aggregation')['metadata']['name'] }}",
        attach_log=True,
    )

    # Medallion Sequence: Bronze → Silver → Gold
    bronze_task >> bronze_sensor >> silver_task >> silver_sensor >> gold_task >> gold_sensor
```

---

## One-Command Deploy (All Manifests)

```bash
# Apply RBAC, ConfigMap, Operator, and Spark Job CRDs
kubectl apply -f kubernetes/spark/spark-operator-rbac.yaml
kubectl apply -f kubernetes/spark/spark-configmap.yaml
kubectl apply -f kubernetes/spark/spark-operator.yaml

# Wait for Operator Readiness
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=spark-operator \
  -n ecommerce-platform --timeout=60s
```

---

## Operations & Monitoring

### Check Spark Applications Status

```bash
kubectl get sparkapplications -n ecommerce-platform
```

Output:

```
NAME                          STATUS    ATTEMPTS   START                  FINISH                 AGE
spark-bronze-ingestion        COMPLETED 1          2026-09-20T12:00:00Z   2026-09-20T12:02:15Z   5m
spark-silver-transformation   COMPLETED 1          2026-09-20T12:02:20Z   2026-09-20T12:05:10Z   3m
spark-gold-aggregation        RUNNING   1          2026-09-20T12:05:15Z   <none>                 1m
```

### Inspect Driver Logs

```bash
kubectl logs -f spark-bronze-ingestion-driver -n ecommerce-platform
```

### Access Spark UI

When a job is running, forward the Driver UI port (4040):

```bash
kubectl port-forward spark-bronze-ingestion-driver 4040:4040 -n ecommerce-platform
```

Open browser at: **`http://localhost:4040`**

### Monitor via k9s

```bash
k9s -n ecommerce-platform
```

Use `k9s` to monitor Driver and Executor pod CPU/Memory usage, view logs (`l`), or exec into containers (`s`).

---

## Cleanup

```bash
# Delete Spark Applications
kubectl delete sparkapp --all -n ecommerce-platform

# Delete Spark Operator & Configs
kubectl delete -f kubernetes/spark/spark-operator.yaml
kubectl delete -f kubernetes/spark/spark-configmap.yaml
kubectl delete -f kubernetes/spark/spark-operator-rbac.yaml
```

