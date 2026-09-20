# Apache Airflow on Kubernetes — Setup Guide

## Architecture Overview

```
                        ┌──────────────────────────────────────────────┐
                        │            Airflow Webserver (UI)            │
                        │           Port 8080 / NodePort 30080         │
                        └──────────────────────┬───────────────────────┘
                                               │
                                               ├──────────────────────┐
                                               ▼                      ▼
  ┌─────────────────────────┐      ┌──────────────────────┐   ┌──────────────┐
  │ Shared PVC:             │      │  Airflow Scheduler   │   │  PostgreSQL  │
  │  - /opt/airflow/dags    │◄────▶│  (LocalExecutor)     │◄─▶│  Metadata DB │
  │  - /opt/airflow/logs    │      │                      │   │  (Port 5432) │
  └─────────────────────────┘      └──────────┬───────────┘   └──────────────┘
                                              │
                         ┌────────────────────┴────────────────────┐
                         │ Triggers Orchestration Pipeline Jobs    │
                         │   - Kafka Ingestion                     │
                         │   - Spark Processing (Bronze/Silver/Gold)│
                         │   - dbt Data Transformations            │
                         └─────────────────────────────────────────┘
```

---

## Docker Image Updates

The custom Airflow Dockerfile in `docker/airflow/DockerFile` has been configured with:

- **Base Image**: Inherits from `ecommerce-platform-base:latest` (`ubuntu:22.04` with Java 17 + Python 3.10)
- **AIRFLOW_HOME**: `/opt/airflow`
- **Installed Packages & Providers**:
  - `apache-airflow==2.10.0`
  - `apache-airflow-providers-postgres`
  - `apache-airflow-providers-apache-spark`
  - `apache-airflow-providers-apache-kafka`
  - `apache-airflow-providers-cncf-kubernetes`
  - `psycopg2-binary`
  - `pyspark==3.5.1`
  - `delta-spark==3.2.0`

---

## Files in This Directory

```
kubernetes/airflow/
├── postgres.yaml          # PostgreSQL database deployment, service (5432), and 5Gi PVC
├── pvc.yaml               # Shared PVCs for DAGs (5Gi) and Logs (5Gi)
├── configmap.yaml         # Airflow environment variables & SQL Alchemy connection
├── init-db-job.yaml       # One-time database migration & Admin user creation job
├── deployment.yaml        # Airflow Webserver and Scheduler deployments
├── service.yaml           # Service exposing Webserver UI on port 8080 (NodePort 30080)
└── README.md              # Setup guide and instructions
```

---

## Step-by-Step Setup

### Step 1 — Verify Namespace

Ensure the platform namespace exists (created during Kafka setup):

```bash
kubectl create namespace ecommerce-platform --dry-run=client -o yaml | kubectl apply -f -
```

### Step 2 — Deploy PostgreSQL Metadata Database

Deploy Postgres for Airflow backend storage:

```bash
kubectl apply -f kubernetes/airflow/postgres.yaml
```

Wait for PostgreSQL to be Ready:

```bash
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=airflow-postgres \
  -n ecommerce-platform --timeout=60s
```

### Step 3 — Apply PVCs & ConfigMap

Create persistent storage for DAGs/Logs and apply Airflow configurations:

```bash
kubectl apply -f kubernetes/airflow/pvc.yaml
kubectl apply -f kubernetes/airflow/configmap.yaml
```

### Step 4 — Run Airflow DB Initialization & Create Admin User

Execute the database migration job:

```bash
kubectl apply -f kubernetes/airflow/init-db-job.yaml
```

Monitor the job completion:

```bash
kubectl logs -f job/airflow-init-db -n ecommerce-platform
```

Default credentials created by the job:
- **Username**: `admin`
- **Password**: `admin`
- **Role**: `Admin`

### Step 5 — Deploy Airflow Webserver & Scheduler

Deploy the Webserver UI and Scheduler components:

```bash
kubectl apply -f kubernetes/airflow/deployment.yaml
kubectl apply -f kubernetes/airflow/service.yaml
```

Wait for pods to become Ready:

```bash
kubectl get pods -n ecommerce-platform -l app.kubernetes.io/name=airflow -w
```

---

## One-Command Deploy

Deploy all Airflow components in sequence:

```bash
# 1. Deploy DB & Storage
kubectl apply -f kubernetes/airflow/postgres.yaml
kubectl apply -f kubernetes/airflow/pvc.yaml
kubectl apply -f kubernetes/airflow/configmap.yaml

# 2. Wait for Postgres
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=airflow-postgres \
  -n ecommerce-platform --timeout=60s

# 3. Initialize Database & Admin User
kubectl apply -f kubernetes/airflow/init-db-job.yaml
kubectl wait --for=condition=complete job/airflow-init-db \
  -n ecommerce-platform --timeout=120s

# 4. Deploy Airflow Services
kubectl apply -f kubernetes/airflow/deployment.yaml
kubectl apply -f kubernetes/airflow/service.yaml
```

---

## Accessing Airflow Webserver UI

### Option A: Via Port-Forwarding (Recommended for local dev)

```bash
kubectl port-forward svc/airflow-webserver 8080:8080 -n ecommerce-platform
```

Open browser at: **`http://localhost:8080`**

### Option B: Via NodePort

If using Minikube / Docker Desktop K8s:
- **Minikube**: `minikube service airflow-webserver -n ecommerce-platform`
- **NodePort URL**: `http://<node-ip>:30080`

Log in using:
- **Username**: `admin`
- **Password**: `admin`

---

## Deploying & Managing DAGs

Place your Airflow DAG python files into the shared volume or copy them into the webserver/scheduler pod:

```bash
# Copy DAGs from local machine into Airflow Webserver pod
kubectl cp pipelines/airflow/dags/ ecommerce-platform/$(kubectl get pod -n ecommerce-platform -l app.kubernetes.io/component=webserver -o jsonpath='{.items[0].metadata.name}'):/opt/airflow/dags/
```

---

## Verification & Health Checks

### Check Pod Status

```bash
kubectl get pods -n ecommerce-platform -l app.kubernetes.io/name=airflow
```

Expected output:

```
NAME                                 READY   STATUS      RESTARTS   AGE
airflow-postgres-xxxxxxxxx-xxxxx     1/1     Running     0          2m
airflow-webserver-xxxxxxxxx-xxxxx    1/1     Running     0          1m
airflow-scheduler-xxxxxxxxx-xxxxx    1/1     Running     0          1m
```

### Check Database Connection from Webserver Pod

```bash
kubectl exec -it deployment/airflow-webserver -n ecommerce-platform -- \
  airflow db check
```

---

## Troubleshooting

| Symptom | Cause | Solution |
|---------|-------|----------|
| `airflow-init-db` pod crashing | PostgreSQL not ready yet | Verify Postgres logs with `kubectl logs deployment/airflow-postgres -n ecommerce-platform` |
| Webserver stuck in `ContainerCreating` | PVC provisioning issue | Check `kubectl describe pvc airflow-dags-pvc -n ecommerce-platform` |
| UI shows `Scheduler is not running` | Scheduler pod errored | Check `kubectl logs deployment/airflow-scheduler -n ecommerce-platform` |

---

## Cleanup

```bash
kubectl delete -f kubernetes/airflow/service.yaml
kubectl delete -f kubernetes/airflow/deployment.yaml
kubectl delete -f kubernetes/airflow/init-db-job.yaml
kubectl delete -f kubernetes/airflow/configmap.yaml
kubectl delete -f kubernetes/airflow/pvc.yaml
kubectl delete -f kubernetes/airflow/postgres.yaml
```

