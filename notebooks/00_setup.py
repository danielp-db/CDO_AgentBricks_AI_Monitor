# Databricks notebook source

# MAGIC %md
# MAGIC # 00 - Setup & AgentBricks Initialization
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Verifies catalog/schema and telemetry data exist
# MAGIC 2. Creates an AgentBricks Custom LLM for the FinOps Assistant
# MAGIC 3. Configures the model with FinOps-specific instructions and training data
# MAGIC 4. Starts optimization (fine-tuning)
# MAGIC
# MAGIC **Prerequisites:** Run `07_generate_telemetry` first to create synthetic data.

# COMMAND ----------

dbutils.widgets.text("catalog", "finops_monitor", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data Exists

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

required_tables = [
    "serving_metrics", "billing_usage", "query_logs",
    "audit_logs", "quality_scores", "training_data"
]

for table in required_tables:
    count = spark.table(f"{CATALOG}.{SCHEMA}.{table}").count()
    assert count > 0, f"Table {table} is empty! Run 07_generate_telemetry first."
    print(f"  {table}: {count} rows")

print("\nAll telemetry tables verified.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create AgentBricks Custom LLM

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

FINOPS_INSTRUCTIONS = """You are the AI Agentic FinOps Assistant, an autonomous monitoring agent
built on Databricks AgentBricks. Your role is to continuously analyze platform telemetry data
for GenAI workloads and provide actionable insights.

Your core capabilities:
1. COST ANALYSIS: Analyze billing data to attribute costs per-agent and per-model. Identify
   trends, anomalies, and optimization opportunities. Always include specific dollar amounts
   and percentage changes.

2. PERFORMANCE MONITORING: Track latency (avg, p50, p95, p99), throughput, and error rates
   for serving endpoints. Validate against SLA thresholds (avg < 1000ms, p95 < 2000ms,
   error rate < 5%). Flag violations immediately.

3. QUALITY EVALUATION: Assess MLflow quality scores (relevance, faithfulness, safety,
   coherence). Detect quality drift by comparing rolling averages against baselines.
   Recommend retraining when scores drop >10% over 7 days.

4. QUERY OPTIMIZATION: Identify expensive queries by duration, bytes scanned, and estimated
   cost. Recommend partition pruning, predicate pushdown, caching, and query rewrites.

5. SECURITY AUDITING: Detect unauthorized access patterns (brute force login attempts,
   unusual token creation, permission changes). Classify severity as CRITICAL, HIGH,
   MEDIUM, or LOW.

6. ANOMALY DETECTION: Use statistical methods to detect outliers across all metrics.
   Report anomalies with baseline values, current values, deviation percentage, and
   correlation with other anomalies.

Response format guidelines:
- Always structure responses with clear sections and bullet points
- Include specific numbers, percentages, and timestamps
- Provide actionable recommendations ranked by impact
- Use severity levels (CRITICAL, HIGH, MEDIUM, LOW) for alerts
- Reference specific agents, endpoints, or resources by name
- When analyzing trends, include week-over-week or day-over-day comparisons
"""

FINOPS_GUIDELINES = [
    "Always include specific dollar amounts and percentages in cost analysis",
    "Flag any SLA violations with exact timestamp and duration",
    "Classify security alerts by severity: CRITICAL, HIGH, MEDIUM, LOW",
    "Provide at least 3 actionable recommendations for each analysis",
    "Correlate anomalies across different metric types when possible",
    "Reference specific agent names, endpoints, and resource IDs",
    "Include baseline vs current values for all anomaly detections",
    "Format tabular data clearly for dashboard display",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create or Update the Custom LLM

# COMMAND ----------

from databricks.sdk.service.agentbricks import CustomLlm, Dataset, Table

LLM_NAME = "finops-assistant-agent"

# Check if already exists by listing and filtering
existing_llm = None
try:
    # Try to find existing LLM by iterating (API may not have direct get-by-name)
    for llm in w.agent_bricks.list_custom_llms():
        if llm.name == LLM_NAME:
            existing_llm = llm
            break
except Exception:
    pass

if existing_llm:
    print(f"Found existing Custom LLM: {existing_llm.name} (ID: {existing_llm.id})")
    print(f"  State: {existing_llm.optimization_state}")
    print(f"  Endpoint: {existing_llm.endpoint_name}")

    # Update instructions if needed
    try:
        updated = w.agent_bricks.update_custom_llm(
            existing_llm.id,
            CustomLlm(
                instructions=FINOPS_INSTRUCTIONS,
                guidelines=FINOPS_GUIDELINES,
            ),
            update_mask="instructions,guidelines"
        )
        print("  Instructions updated.")
    except Exception as e:
        print(f"  Could not update (may be optimizing): {e}")

    llm = existing_llm
else:
    print(f"Creating new Custom LLM: {LLM_NAME}")
    llm = w.agent_bricks.create_custom_llm(
        name=LLM_NAME,
        instructions=FINOPS_INSTRUCTIONS,
        datasets=[
            Dataset(table=Table(
                table_path=f"{CATALOG}.{SCHEMA}.training_data",
                request_col="request",
                response_col="response",
            ))
        ],
        guidelines=FINOPS_GUIDELINES,
        agent_artifact_path=f"{CATALOG}.{SCHEMA}",
    )
    print(f"  Created! ID: {llm.id}")

# Store LLM ID for other notebooks
dbutils.jobs.taskValues.set(key="agentbricks_llm_id", value=str(llm.id))
dbutils.jobs.taskValues.set(key="agentbricks_llm_name", value=LLM_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start Optimization

# COMMAND ----------

import time

if llm.optimization_state in (None, "CREATED", "FAILED", "CANCELLED"):
    print("Starting optimization...")
    try:
        optimized = w.agent_bricks.start_optimize(llm.id)
        print(f"  Optimization started. State: {optimized.optimization_state}")
    except Exception as e:
        print(f"  Could not start optimization: {e}")
        print("  This is OK - the demo will use a foundation model as fallback.")
elif llm.optimization_state == "COMPLETED":
    print(f"Optimization already completed. Endpoint: {llm.endpoint_name}")
elif llm.optimization_state in ("PENDING", "RUNNING"):
    print(f"Optimization already in progress. State: {llm.optimization_state}")
else:
    print(f"Current state: {llm.optimization_state}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Configuration for Other Notebooks

# COMMAND ----------

config = {
    "catalog": CATALOG,
    "schema": SCHEMA,
    "llm_name": LLM_NAME,
    "llm_id": str(llm.id) if llm else None,
    "endpoint_name": llm.endpoint_name if llm and llm.endpoint_name else None,
    "fallback_endpoint": "databricks-meta-llama-3-3-70b-instruct",
}

import json
print("\nFinOps Assistant Configuration:")
print(json.dumps(config, indent=2))
print("\nSetup complete! The monitoring jobs can now run.")
