# Monitoring Platform — Prometheus, Loki, Promtail & Grafana

## Architecture Overview

```
                                  ┌──────────────────────────────────────────────┐
                                  │            Grafana Dashboard (UI)            │
                                  │           Port 3000 / NodePort 30000         │
                                  └──────────────────────┬───────────────────────┘
                                                         │
                           ┌─────────────────────────────┴─────────────────────────────┐
                           ▼                                                           ▼
            ┌─────────────────────────────┐                             ┌─────────────────────────────┐
            │   Prometheus Server (9090)  │                             │      Loki Log Store (3100)  │
            │   Metrics & Time Series     │                             │      Log Aggregation        │
            └──────────────┬──────────────┘                             └──────────────┬──────────────┘
                           │                                                           │
        ┌──────────────────┴──────────────────┐                     ┌──────────────────┴──────────────────┐
        │  Prometheus Pod Auto-Discovery /   │                     │  Promtail DaemonSet Log Collectors  │
        │  cAdvisor Node & Pod Scraping       │                     │  (/var/log/pods/*/*/*.log)          │
        └─────────────────────────────────────┘                     └─────────────────────────────────────┘
                                       │                                           │
                                       ▼                                           ▼
            ┌────────────────────────────────────────────────────────────────────────────────────────┐
            │                     Ecommerce Platform Pods & Jobs (ecommerce-platform)               │
            │  Kafka Brokers | Airflow Pods | Spark Drivers & Executors | MinIO | ClickHouse | etc.  │
            └────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Directory Structure

```
monitoring/
├── namespace.yaml                  # Creates `monitoring` namespace
├── prometheus/
│   ├── rbac.yaml                   # ClusterRole & ServiceAccount for Prometheus scraping
│   ├── configmap.yaml              # prometheus.yml scrape configs (nodes, cAdvisor, pods)
│   ├── pvc.yaml                    # Persistent Storage for Prometheus (5Gi)
│   └── deployment.yaml             # Prometheus Deployment & Service (Port 9090)
├── loki/
│   ├── loki-configmap.yaml         # loki.yaml configuration
│   ├── loki-deployment.yaml        # Loki Deployment, Service (Port 3100), and PVC (5Gi)
│   ├── promtail-configmap.yaml     # promtail.yaml configuration & K8s relabeling rules
│   └── promtail-daemonset.yaml     # Promtail DaemonSet & ClusterRole for node log collection
├── grafana/
│   ├── datasources-configmap.yaml  # Auto-configures Prometheus & Loki datasources
│   ├── dashboards-provider-configmap.yaml # Configures JSON dashboard loader
│   ├── pvc.yaml                    # Persistent Storage for Grafana (2Gi)
│   └── deployment.yaml             # Grafana Deployment & Service (Port 3000 / NodePort 30000)
├── dashboards/
│   └── dashboards-configmap.yaml   # Pre-packaged JSON dashboards for CPU/Memory, Loki logs, & Health
└── README.md                       # Setup guide & instructions
```

---

## Step-by-Step Deployment Guide

### Step 1 — Create the `monitoring` Namespace

```bash
kubectl apply -f monitoring/namespace.yaml
```

### Step 2 — Deploy Prometheus

Deploy Prometheus RBAC, ConfigMap, PVC, Deployment, and Service:

```bash
kubectl apply -f monitoring/prometheus/rbac.yaml
kubectl apply -f monitoring/prometheus/configmap.yaml
kubectl apply -f monitoring/prometheus/pvc.yaml
kubectl apply -f monitoring/prometheus/deployment.yaml
```

Verify Prometheus readiness:

```bash
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=prometheus \
  -n monitoring --timeout=60s
```

### Step 3 — Deploy Loki & Promtail Log Aggregation

Deploy Loki log store and Promtail node-level log collector:

```bash
kubectl apply -f monitoring/loki/loki-configmap.yaml
kubectl apply -f monitoring/loki/loki-deployment.yaml
kubectl apply -f monitoring/loki/promtail-configmap.yaml
kubectl apply -f monitoring/loki/promtail-daemonset.yaml
```

Verify Loki & Promtail readiness:

```bash
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=loki \
  -n monitoring --timeout=60s
kubectl get daemonset promtail -n monitoring
```

### Step 4 — Deploy Grafana & Pre-loaded Dashboards

Apply Grafana provisioning and dashboards:

```bash
kubectl apply -f monitoring/grafana/datasources-configmap.yaml
kubectl apply -f monitoring/grafana/dashboards-provider-configmap.yaml
kubectl apply -f monitoring/dashboards/dashboards-configmap.yaml
kubectl apply -f monitoring/grafana/deployment.yaml
```

Verify Grafana readiness:

```bash
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/name=grafana \
  -n monitoring --timeout=60s
```

---

## One-Command Deployment (Deploy Entire Monitoring Stack)

```bash
# Apply all monitoring manifests in sequence
kubectl apply -f monitoring/namespace.yaml
kubectl apply -f monitoring/prometheus/rbac.yaml
kubectl apply -f monitoring/prometheus/configmap.yaml
kubectl apply -f monitoring/prometheus/pvc.yaml
kubectl apply -f monitoring/prometheus/deployment.yaml

kubectl apply -f monitoring/loki/loki-configmap.yaml
kubectl apply -f monitoring/loki/loki-deployment.yaml
kubectl apply -f monitoring/loki/promtail-configmap.yaml
kubectl apply -f monitoring/loki/promtail-daemonset.yaml

kubectl apply -f monitoring/grafana/datasources-configmap.yaml
kubectl apply -f monitoring/grafana/dashboards-provider-configmap.yaml
kubectl apply -f monitoring/dashboards/dashboards-configmap.yaml
kubectl apply -f monitoring/grafana/deployment.yaml
```

---

## Accessing Grafana & Monitoring Dashboards

### Access Grafana UI

- **Port-Forwarding (Recommended for local dev)**:
  ```bash
  kubectl port-forward svc/grafana 3000:3000 -n monitoring
  ```
  Open browser at: **`http://localhost:3000`**

- **NodePort Access**:
  - Open: **`http://<node-ip>:30000`**

- **Login Credentials**:
  - **Username**: `admin`
  - **Password**: `admin`

---

## Pre-Configured Dashboards

Upon logging into Grafana, navigate to **Dashboards → Ecommerce Data Platform**:

1. **Kubernetes Pod & Node Metrics** (`k8s-cluster-cpu-memory.json`):
   - Real-time CPU usage by Pod & Container
   - Memory Working Set Usage by Pod
   - Network I/O (Rx/Tx bytes per second)

2. **Pod & Job Log Streams** (`loki-pod-job-logs.json`):
   - Real-time log streaming from all pods in `ecommerce-platform` and `monitoring` namespaces
   - Filter by namespace, pod name, container, or job name
   - Search logs using LogQL syntax (e.g., `{namespace="ecommerce-platform"} |= "error"`)

3. **Platform Services Health** (`platform-services-health.json`):
   - Active Pod Count across namespaces
   - Kafka Broker pod health status
   - Airflow Scheduler & Webserver health

---

## Querying Pod Logs with LogQL (Loki)

In Grafana **Explore** tab (selecting **Loki** datasource):

```logql
# Stream all logs from ecommerce-platform namespace
{namespace="ecommerce-platform"}

# Stream logs specifically from Spark Driver or Executor pods
{namespace="ecommerce-platform", pod=~"spark.*"}

# Filter Airflow logs containing errors or warnings
{namespace="ecommerce-platform", pod=~"airflow.*"} |= "ERROR"

# Search Kafka logs for topic partition updates
{namespace="ecommerce-platform", pod=~"kafka.*"} |= "topic"
```

---

## Operations & Troubleshooting

### Check Pod Status in Monitoring Namespace

```bash
kubectl get pods -n monitoring
```

Expected output:

```
NAME                          READY   STATUS    RESTARTS   AGE
grafana-xxxxxxxxx-xxxxx       1/1     Running   0          2m
loki-xxxxxxxxx-xxxxx          1/1     Running   0          3m
prometheus-xxxxxxxxx-xxxxx    1/1     Running   0          3m
promtail-xxxxx                1/1     Running   0          3m
```

### Access Prometheus UI Directly

```bash
kubectl port-forward svc/prometheus 9090:9090 -n monitoring
# Open http://localhost:9090
```

---

## Cleanup

```bash
# Delete all monitoring resources
kubectl delete namespace monitoring
```

