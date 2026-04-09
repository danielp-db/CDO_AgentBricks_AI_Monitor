# AI Agentic FinOps Assistant

An autonomous AI agent built using **Databricks AgentBricks** that continuously monitors, analyzes, and acts on platform telemetry data to ensure GenAI workloads are cost-effective, high-performing, and properly governed.

Deployed as a **Databricks Asset Bundle (DAB)** to workspace: `fevm-att-log-anomaly.cloud.databricks.com`

---

## Architecture

```
                    ┌──────────────────────────────┐
                    │      Databricks App           │
                    │   (FastAPI + Dashboard UI)    │
                    │                                │
                    │  ┌─────────┐  ┌────────────┐  │
                    │  │ Charts  │  │ NL Chat    │  │
                    │  │ Tables  │  │ Interface  │  │
                    │  └────┬────┘  └─────┬──────┘  │
                    └───────┼─────────────┼─────────┘
                            │             │
                    ┌───────▼─────────────▼─────────┐
                    │     AgentBricks Custom LLM     │
                    │    (finops-assistant-agent)     │
                    │   Fine-tuned FinOps Specialist  │
                    └───────────────┬────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          │                         │                         │
    ┌─────▼──────┐          ┌──────▼───────┐          ┌──────▼───────┐
    │  Scheduled  │          │   Results    │          │  Telemetry   │
    │    Jobs     │          │   Tables     │          │    Data      │
    │  (6 cadences)│         │  (Delta)     │          │  (Delta)     │
    └─────────────┘          └──────────────┘          └──────────────┘
```

### Components

| Component | Description |
|-----------|-------------|
| **AgentBricks Custom LLM** | Fine-tuned FinOps specialist created via `w.agent_bricks.create_custom_llm()` with domain-specific instructions and training data |
| **7 Monitoring Notebooks** | Each capability runs as a scheduled job, queries telemetry, calls the AgentBricks agent, and stores structured results |
| **Databricks App** | FastAPI backend + HTML/JS dashboard with 7 tabs (Overview, Costs, Performance, Quality, Security, Anomalies, Query Optimization) and a natural language chat interface |
| **DAB Configuration** | Infrastructure-as-code: jobs, app, schedules, and parameters all defined in YAML |

---

## Capabilities & Job Schedules

| # | Capability | Notebook | Cron Schedule | Description |
|---|-----------|----------|---------------|-------------|
| 1 | Cost Analysis | `01_cost_analysis.py` | `0 0 * * * ?` (Hourly) | Per-agent, per-model cost attribution and trend analysis |
| 2 | Performance Monitoring | `02_performance_monitoring.py` | `0 */15 * * * ?` (Every 15 min) | Latency, throughput, error rate tracking with SLA validation |
| 3 | Quality Evaluation | `03_quality_evaluation.py` | `0 0 8 * * ?` (Daily 8 AM) | MLflow-as-judge quality scores, drift detection |
| 4 | Query Optimization | `04_query_optimization.py` | `0 0 6 * * ?` (Daily 6 AM) | Identifies expensive queries, suggests optimizations |
| 5 | Security Auditing | `05_security_auditing.py` | `0 */5 * * * ?` (Every 5 min) | Unauthorized access detection, permission drift |
| 6 | Anomaly Detection | `06_anomaly_detection.py` | `0 */10 * * * ?` (Every 10 min) | Cross-dimensional anomaly detection with root cause analysis |
| 7 | Natural Language Interface | App `/api/chat` | On-demand | Ask questions in plain English via the dashboard chat |

---

## Prerequisites

1. **Databricks CLI** v0.295+ installed and configured
2. **Workspace access** to `fevm-att-log-anomaly.cloud.databricks.com`
3. **Unity Catalog** enabled on the workspace
4. **SQL Warehouse** available for the app and queries
5. **Model Serving** endpoint available (e.g., `databricks-meta-llama-3-3-70b-instruct`)

---

## Step-by-Step Deployment Guide

### Step 1: Authenticate to the Workspace

```bash
# Verify your Databricks CLI profile
databricks auth login --host https://fevm-att-log-anomaly.cloud.databricks.com --profile fevm-att-log-anomaly

# Verify connectivity
databricks workspace list / --profile fevm-att-log-anomaly
```

### Step 2: Review the Configuration

The DAB is configured in `databricks.yml`:
- **Bundle name**: `finops-ai-assistant`
- **Variables**: `catalog` (default: `att_log_anomaly_catalog`), `schema` (default: `finops_monitor`), `warehouse_id`
- **Targets**: `dev` and `prod` pointing to the workspace

Review and update the `warehouse_id` variable:
```bash
# List available warehouses
databricks warehouses list --profile fevm-att-log-anomaly

# Note the ID of the warehouse you want to use
```

### Step 3: Validate the Bundle

```bash
cd /Users/daniel.perez/VibeCode/CDO_AgentBricks_AI_Monitor

databricks bundle validate \
  --profile fevm-att-log-anomaly \
  --var warehouse_id=<YOUR_WAREHOUSE_ID>
```

### Step 4: Deploy the Bundle

```bash
databricks bundle deploy \
  --profile fevm-att-log-anomaly \
  --var warehouse_id=<YOUR_WAREHOUSE_ID> \
  --target dev
```

This deploys:
- 7 scheduled jobs (one per monitoring capability + setup)
- 1 Databricks App (the dashboard)
- All notebooks to the workspace

### Step 5: Run Initial Setup

Run the setup job first to generate telemetry data and initialize AgentBricks:

```bash
# Run the setup job (generates data + creates AgentBricks Custom LLM)
databricks bundle run finops_setup \
  --profile fevm-att-log-anomaly \
  --var warehouse_id=<YOUR_WAREHOUSE_ID>
```

This job executes two tasks in sequence:
1. **`07_generate_telemetry`** - Creates the catalog/schema and populates synthetic telemetry data:
   - `serving_metrics` - 7 days of endpoint performance data
   - `billing_usage` - 30 days of cost data per agent/SKU
   - `query_logs` - 14 days of query execution logs
   - `audit_logs` - 7 days of security audit events
   - `quality_scores` - 30 days of quality evaluation scores
   - `training_data` - 10 FinOps Q&A training pairs

2. **`00_setup`** - Creates the AgentBricks Custom LLM:
   - Verifies all data tables exist
   - Calls `w.agent_bricks.create_custom_llm()` with FinOps-specific instructions
   - Attaches the training dataset
   - Starts optimization (fine-tuning)

### Step 6: Run Individual Monitoring Jobs

After setup completes, test each monitoring capability:

```bash
# Cost Analysis
databricks bundle run finops_cost_analysis --profile fevm-att-log-anomaly --var warehouse_id=<WH_ID>

# Performance Monitoring
databricks bundle run finops_performance_monitoring --profile fevm-att-log-anomaly --var warehouse_id=<WH_ID>

# Quality Evaluation
databricks bundle run finops_quality_evaluation --profile fevm-att-log-anomaly --var warehouse_id=<WH_ID>

# Query Optimization
databricks bundle run finops_query_optimization --profile fevm-att-log-anomaly --var warehouse_id=<WH_ID>

# Security Auditing
databricks bundle run finops_security_auditing --profile fevm-att-log-anomaly --var warehouse_id=<WH_ID>

# Anomaly Detection
databricks bundle run finops_anomaly_detection --profile fevm-att-log-anomaly --var warehouse_id=<WH_ID>
```

Once tested, the scheduled cron triggers will run them automatically.

### Step 7: Access the Dashboard App

After deployment, the app is available at the URL shown in the deploy output.

You can also find it via:
```bash
databricks apps list --profile fevm-att-log-anomaly
```

The dashboard has 8 tabs:
1. **Overview** - Summary cards, cost trend chart, latency chart, SLA status table
2. **Cost Analysis** - Daily cost by agent, cost by SKU, AI-generated analysis text
3. **Performance** - 24h latency trends, endpoint SLA status details
4. **Quality** - 30-day quality score trends, drift detection table
5. **Security** - Alert severity cards, security alert details table
6. **Anomalies** - Detected anomaly list with deviation percentages
7. **Query Optimization** - Expensive queries with optimization suggestions
8. **Ask AI** - Natural language chat powered by the AgentBricks FinOps agent

---

## How AgentBricks Is Used

### 1. Custom LLM Creation (`00_setup.py`)

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.agentbricks import CustomLlm, Dataset, Table

w = WorkspaceClient()
llm = w.agent_bricks.create_custom_llm(
    name="finops-assistant-agent",
    instructions="You are the AI Agentic FinOps Assistant...",
    datasets=[Dataset(table=Table(
        table_path="att_log_anomaly_catalog.finops_monitor.training_data",
        request_col="request",
        response_col="response",
    ))],
    guidelines=["Always include dollar amounts...", ...],
    agent_artifact_path="att_log_anomaly_catalog.finops_monitor",
)
w.agent_bricks.start_optimize(llm.id)
```

### 2. Agent Invocation (All Monitoring Notebooks)

Each notebook calls the AgentBricks-created serving endpoint with telemetry context:

```python
response = w.serving_endpoints.query(
    name="finops-assistant-agent",  # AgentBricks endpoint
    messages=[
        ChatMessage(role=ChatMessageRole.SYSTEM, content="You are the FinOps Assistant..."),
        ChatMessage(role=ChatMessageRole.USER, content=f"Analyze: {telemetry_data}"),
    ],
    max_tokens=4000,
    temperature=0.3,
)
```

### 3. Natural Language Chat (App `/api/chat`)

The dashboard chat endpoint enriches user questions with live monitoring context before calling the AgentBricks agent, enabling conversational access to all FinOps data.

---

## Data Model

### Source Tables (Synthetic Telemetry)

| Table | Description | Update Frequency |
|-------|-------------|------------------|
| `serving_metrics` | Endpoint latency, throughput, errors, tokens | Generated once; 7 days of hourly data |
| `billing_usage` | Per-agent, per-SKU cost and DBU usage | Generated once; 30 days of daily data |
| `query_logs` | SQL query execution history | Generated once; 14 days |
| `audit_logs` | Security/access audit events | Generated once; 7 days |
| `quality_scores` | MLflow quality evaluation scores | Generated once; 30 days |
| `training_data` | AgentBricks training Q&A pairs | Generated once; ~10 pairs |

### Results Tables (Agent Output)

| Table | Written By | Description |
|-------|-----------|-------------|
| `cost_analysis_results` | Notebook 01 | Weekly summaries, per-agent findings, recommendations |
| `performance_results` | Notebook 02 | SLA status, latency, error rate per endpoint |
| `quality_results` | Notebook 03 | Quality scores, drift detection per endpoint |
| `query_optimization_results` | Notebook 04 | Expensive queries with optimization suggestions |
| `security_results` | Notebook 05 | Security alerts by severity and type |
| `anomaly_results` | Notebook 06 | Detected anomalies with deviation metrics |
| `chat_history` | App | User chat messages and agent responses |

---

## Project Structure

```
CDO_AgentBricks_AI_Monitor/
├── databricks.yml              # DAB root configuration
├── PROMPT.md                   # Original requirements document
├── README.md                   # This file
├── resources/
│   ├── jobs.yml                # 7 job definitions with cron schedules
│   └── app.yml                 # Databricks App configuration
├── notebooks/
│   ├── 00_setup.py             # AgentBricks Custom LLM setup
│   ├── 01_cost_analysis.py     # Hourly cost analysis
│   ├── 02_performance_monitoring.py  # 15-min SLA monitoring
│   ├── 03_quality_evaluation.py     # Daily quality/drift check
│   ├── 04_query_optimization.py     # Daily query analysis
│   ├── 05_security_auditing.py      # 5-min security scan
│   ├── 06_anomaly_detection.py      # 10-min anomaly detection
│   └── 07_generate_telemetry.py     # Synthetic data generator
└── src/
    └── app/
        ├── app.yaml            # App deployment config
        ├── requirements.txt    # Python dependencies
        ├── backend/
        │   ├── __init__.py
        │   └── main.py         # FastAPI backend (API + chat)
        └── static/
            └── index.html      # Dashboard UI (single-page app)
```

---

## Cleanup

To remove all deployed resources:

```bash
databricks bundle destroy \
  --profile fevm-att-log-anomaly \
  --var warehouse_id=<YOUR_WAREHOUSE_ID> \
  --target dev
```

To also remove the data:
```sql
DROP SCHEMA IF EXISTS att_log_anomaly_catalog.finops_monitor CASCADE;
```
