# Databricks notebook source

# MAGIC %md
# MAGIC # 07 - Generate Synthetic Telemetry Data
# MAGIC
# MAGIC Generates realistic synthetic telemetry data that simulates:
# MAGIC - Model serving endpoint metrics (latency, throughput, errors)
# MAGIC - Billing/usage records per agent and model
# MAGIC - Query execution logs
# MAGIC - Audit/access logs
# MAGIC - Quality evaluation scores
# MAGIC
# MAGIC This data powers all FinOps monitoring capabilities.

# COMMAND ----------

dbutils.widgets.text("catalog", "finops_monitor", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Using {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Serving Endpoint Metrics

# COMMAND ----------

import random
import json
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType, LongType
)

ENDPOINTS = [
    "finops-assistant-agent",
    "customer-support-agent",
    "code-review-agent",
    "data-analysis-agent",
    "document-summarizer",
]

MODELS = [
    "databricks-meta-llama-3-3-70b-instruct",
    "databricks-claude-sonnet-4-6",
    "databricks-dbrx-instruct",
    "finops-custom-llm-v1",
]

now = datetime.utcnow()
rows = []

for hour_offset in range(168):  # 7 days of data
    ts = now - timedelta(hours=hour_offset)
    for endpoint in ENDPOINTS:
        model = random.choice(MODELS)
        base_latency = random.uniform(200, 800)

        # Inject anomalies: occasional spikes
        is_spike = random.random() < 0.05
        latency_multiplier = random.uniform(3, 8) if is_spike else 1.0

        request_count = random.randint(50, 500)
        error_count = int(request_count * random.uniform(0.001, 0.05 if not is_spike else 0.15))

        rows.append(Row(
            timestamp=ts,
            endpoint_name=endpoint,
            model_name=model,
            request_count=request_count,
            avg_latency_ms=round(base_latency * latency_multiplier, 2),
            p50_latency_ms=round(base_latency * latency_multiplier * 0.8, 2),
            p95_latency_ms=round(base_latency * latency_multiplier * 1.5, 2),
            p99_latency_ms=round(base_latency * latency_multiplier * 2.2, 2),
            error_count=error_count,
            error_rate=round(error_count / max(request_count, 1), 4),
            total_tokens=random.randint(50000, 500000),
            input_tokens=random.randint(30000, 300000),
            output_tokens=random.randint(20000, 200000),
            is_anomaly=is_spike,
        ))

df_metrics = spark.createDataFrame(rows)
df_metrics.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.serving_metrics")
print(f"Wrote {df_metrics.count()} serving metric records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Billing / Cost Data

# COMMAND ----------

SKU_COSTS = {
    "STANDARD_ALL_PURPOSE_COMPUTE": 0.40,
    "PREMIUM_ALL_PURPOSE_COMPUTE": 0.55,
    "JOBS_COMPUTE": 0.15,
    "SERVERLESS_SQL": 0.70,
    "MODEL_SERVING": 0.08,
    "FOUNDATION_MODEL_SERVING": 0.02,
}

AGENTS = [
    "finops-assistant",
    "customer-support",
    "code-review",
    "data-analysis",
    "document-summarizer",
    "security-scanner",
]

cost_rows = []
for day_offset in range(30):  # 30 days
    date = (now - timedelta(days=day_offset)).date()
    for agent in AGENTS:
        for sku, unit_price in SKU_COSTS.items():
            dbu_usage = random.uniform(10, 500)
            # Inject cost spikes
            if random.random() < 0.03:
                dbu_usage *= random.uniform(5, 15)

            cost_rows.append(Row(
                usage_date=datetime.combine(date, datetime.min.time()),
                agent_name=agent,
                sku_name=sku,
                dbu_usage=round(dbu_usage, 2),
                unit_price=unit_price,
                total_cost=round(dbu_usage * unit_price, 2),
                workspace_id="fevm-att-log-anomaly",
                cloud="AZURE",
            ))

df_costs = spark.createDataFrame(cost_rows)
df_costs.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.billing_usage")
print(f"Wrote {df_costs.count()} billing records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Query Execution Logs

# COMMAND ----------

QUERY_TEMPLATES = [
    "SELECT * FROM {table} WHERE date > '{date}'",
    "SELECT agent_name, SUM(total_cost) FROM billing_usage GROUP BY agent_name",
    "SELECT COUNT(*) FROM serving_metrics WHERE error_rate > 0.05",
    "SELECT endpoint_name, AVG(avg_latency_ms) FROM serving_metrics GROUP BY 1",
    "INSERT INTO results SELECT * FROM staging_table",
    "CREATE TABLE tmp_{table} AS SELECT * FROM {table} WHERE 1=1",
    "SELECT * FROM serving_metrics sm JOIN billing_usage bu ON sm.endpoint_name = bu.agent_name",
    "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET *",
    "SELECT *, ROW_NUMBER() OVER (PARTITION BY agent_name ORDER BY total_cost DESC) FROM billing_usage",
    "WITH cte AS (SELECT * FROM serving_metrics WHERE timestamp > current_timestamp() - INTERVAL 1 HOUR) SELECT * FROM cte",
]

USERS = [
    "daniel.perez@databricks.com",
    "service-account-finops@databricks.com",
    "ml-pipeline@databricks.com",
    "analytics-team@databricks.com",
    "admin@databricks.com",
]

query_rows = []
for day_offset in range(14):
    date = now - timedelta(days=day_offset)
    for _ in range(random.randint(20, 100)):
        ts = date - timedelta(minutes=random.randint(0, 1440))
        query = random.choice(QUERY_TEMPLATES).format(
            table=random.choice(["serving_metrics", "billing_usage", "quality_scores"]),
            date=(now - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d"),
        )
        duration_ms = random.uniform(100, 30000)
        # Some expensive queries
        if random.random() < 0.1:
            duration_ms *= random.uniform(5, 20)

        bytes_scanned = int(duration_ms * random.uniform(1000, 50000))

        query_rows.append(Row(
            timestamp=ts,
            query_id=f"q-{random.randint(100000, 999999)}",
            query_text=query,
            user_name=random.choice(USERS),
            duration_ms=round(duration_ms, 2),
            bytes_scanned=bytes_scanned,
            rows_returned=random.randint(0, 100000),
            status=random.choice(["COMPLETED", "COMPLETED", "COMPLETED", "FAILED", "CANCELLED"]),
            warehouse_id="wh-finops-001",
            estimated_cost=round(duration_ms * 0.0001, 4),
        ))

df_queries = spark.createDataFrame(query_rows)
df_queries.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.query_logs")
print(f"Wrote {df_queries.count()} query log records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Audit / Access Logs

# COMMAND ----------

ACTIONS = [
    ("workspace.access", "LOGIN"),
    ("workspace.access", "LOGIN_FAILED"),
    ("secrets.getSecret", "READ"),
    ("clusters.create", "WRITE"),
    ("clusters.delete", "WRITE"),
    ("jobs.create", "WRITE"),
    ("jobs.run", "EXECUTE"),
    ("sql.executeStatement", "EXECUTE"),
    ("unityCatalog.getTable", "READ"),
    ("unityCatalog.createTable", "WRITE"),
    ("permissions.changePermission", "ADMIN"),
    ("tokens.create", "ADMIN"),
    ("endpoints.create", "WRITE"),
    ("endpoints.query", "EXECUTE"),
]

SOURCES = ["UI", "API", "CLI", "DBSQL", "NOTEBOOK"]

audit_rows = []
for day_offset in range(7):
    date = now - timedelta(days=day_offset)
    for _ in range(random.randint(100, 500)):
        ts = date - timedelta(minutes=random.randint(0, 1440))
        action, action_type = random.choice(ACTIONS)
        user = random.choice(USERS)

        # Inject suspicious events
        is_suspicious = False
        if action == "workspace.access" and action_type == "LOGIN_FAILED":
            is_suspicious = random.random() < 0.3
        if action == "permissions.changePermission":
            is_suspicious = random.random() < 0.4
        if action == "tokens.create":
            is_suspicious = random.random() < 0.2

        audit_rows.append(Row(
            timestamp=ts,
            user_name=user,
            action=action,
            action_type=action_type,
            source=random.choice(SOURCES),
            status="SUCCESS" if random.random() > 0.1 else "FAILURE",
            ip_address=f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
            user_agent=f"databricks-sdk-python/{random.choice(['0.35.0', '0.36.0', '0.37.0'])}",
            is_suspicious=is_suspicious,
            resource_id=f"res-{random.randint(1000, 9999)}",
        ))

df_audit = spark.createDataFrame(audit_rows)
df_audit.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.audit_logs")
print(f"Wrote {df_audit.count()} audit log records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Quality Evaluation Scores

# COMMAND ----------

quality_rows = []
for day_offset in range(30):
    date = (now - timedelta(days=day_offset)).date()
    for endpoint in ENDPOINTS:
        base_quality = random.uniform(0.7, 0.95)
        # Quality drift: gradual degradation for some endpoints
        drift = day_offset * random.uniform(0, 0.003) if random.random() < 0.3 else 0

        quality_rows.append(Row(
            eval_date=datetime.combine(date, datetime.min.time()),
            endpoint_name=endpoint,
            relevance_score=round(max(0, base_quality - drift + random.uniform(-0.05, 0.05)), 4),
            faithfulness_score=round(max(0, base_quality - drift * 0.5 + random.uniform(-0.05, 0.05)), 4),
            safety_score=round(min(1.0, random.uniform(0.85, 1.0)), 4),
            coherence_score=round(max(0, base_quality + random.uniform(-0.08, 0.08)), 4),
            overall_score=round(max(0, base_quality - drift * 0.7 + random.uniform(-0.03, 0.03)), 4),
            eval_count=random.randint(50, 200),
            drift_detected=drift > 0.02,
            judge_model="databricks-meta-llama-3-3-70b-instruct",
        ))

df_quality = spark.createDataFrame(quality_rows)
df_quality.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.quality_scores")
print(f"Wrote {df_quality.count()} quality evaluation records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate AgentBricks Training Data

# COMMAND ----------

training_pairs = [
    {"input": "What is the total cost for the customer-support agent this week?",
     "output": "Based on the billing data, the customer-support agent consumed $2,847.32 this week across all SKUs. The largest cost driver was FOUNDATION_MODEL_SERVING at $1,523.18 (53.5%), followed by SERVERLESS_SQL at $892.14 (31.3%). This represents a 12% increase from last week."},
    {"input": "Are there any latency SLA violations?",
     "output": "Yes, I detected 3 SLA violations in the last 24 hours:\n1. code-review-agent: p95 latency of 2,450ms exceeded the 2,000ms SLA at 14:23 UTC\n2. document-summarizer: p99 latency spike to 5,200ms at 09:15 UTC (anomaly detected)\n3. customer-support-agent: avg latency of 1,100ms exceeded 1,000ms SLA between 11:00-11:30 UTC\nRecommendation: Scale up the code-review-agent endpoint and investigate the document-summarizer spike."},
    {"input": "Which agents have quality drift?",
     "output": "Quality drift detected in 2 agents over the past 14 days:\n1. document-summarizer: relevance score dropped from 0.91 to 0.78 (-14.3%). Drift started ~5 days ago.\n2. data-analysis-agent: faithfulness score declined from 0.88 to 0.82 (-6.8%). Gradual decline over 10 days.\nAll other agents maintain stable quality scores within normal variance. Recommend retraining document-summarizer with updated evaluation data."},
    {"input": "Show me the most expensive queries today",
     "output": "Top 5 most expensive queries today:\n1. Full table scan on serving_metrics (32.4s, 1.2TB scanned) - $12.84 - Run by analytics-team\n2. Cross-join between serving_metrics and billing_usage (28.1s, 890GB) - $11.24 - Run by ml-pipeline\n3. Unoptimized GROUP BY on billing_usage (15.7s, 456GB) - $6.28 - Run by service-account-finops\n4. Recursive CTE on query_logs (12.3s, 234GB) - $4.92 - Run by daniel.perez\n5. MERGE INTO with missing predicate (9.8s, 178GB) - $3.92 - Run by admin\nTotal estimated cost: $39.20. Recommendations: Add partition pruning to query #1, rewrite #2 join condition."},
    {"input": "Are there any security concerns?",
     "output": "Security audit flagged 4 concerns:\n1. HIGH: 15 failed login attempts from IP 10.42.88.12 for user admin@databricks.com in the last hour - possible brute force\n2. MEDIUM: tokens.create called 8 times by service-account-finops - unusual token generation pattern\n3. MEDIUM: permissions.changePermission on 3 Unity Catalog tables by analytics-team - verify authorization\n4. LOW: secrets.getSecret accessed 47 times by ml-pipeline - high frequency but from known service account\nImmediate action recommended for concern #1. Verify concerns #2 and #3 with respective teams."},
    {"input": "What anomalies were detected in the last 6 hours?",
     "output": "Anomaly Detection Summary (last 6 hours):\n- COST: Spike of 340% for data-analysis-agent MODEL_SERVING costs at 08:00 UTC. Estimated excess: $456.78\n- LATENCY: document-summarizer p99 latency jumped from 1,200ms baseline to 5,200ms at 09:15 UTC (4.3x increase)\n- ERROR RATE: code-review-agent error rate increased from 1.2% to 8.7% between 10:00-10:30 UTC\n- QUALITY: No quality anomalies detected\n- SECURITY: Brute force pattern detected (see security alerts)\nRoot cause hypothesis: The cost and latency spikes correlate with a deployment at 07:45 UTC. Recommend reviewing recent model updates."},
    {"input": "Compare costs between this week and last week",
     "output": "Week-over-week cost comparison:\n| Agent | This Week | Last Week | Change |\n|-------|-----------|-----------|--------|\n| finops-assistant | $1,234 | $1,180 | +4.6% |\n| customer-support | $2,847 | $2,541 | +12.0% |\n| code-review | $1,567 | $1,623 | -3.5% |\n| data-analysis | $3,210 | $1,890 | +69.8% |\n| document-summarizer | $987 | $1,012 | -2.5% |\nTotal: $9,845 vs $8,246 (+19.4%). The data-analysis-agent increase is the primary driver. Investigation shows a new batch job was added consuming 3x more DBUs."},
    {"input": "What is the token usage breakdown by model?",
     "output": "Token usage (last 7 days):\n| Model | Input Tokens | Output Tokens | Total | Cost |\n|-------|-------------|---------------|-------|------|\n| llama-3-3-70b | 12.4M | 4.2M | 16.6M | $332 |\n| claude-sonnet | 8.7M | 3.1M | 11.8M | $354 |\n| dbrx-instruct | 5.2M | 1.8M | 7.0M | $140 |\n| finops-custom-v1 | 2.1M | 0.9M | 3.0M | $60 |\nTotal: 38.4M tokens, $886. Claude-sonnet has highest cost-per-token ratio. Consider migrating low-complexity tasks to llama or dbrx."},
    {"input": "Give me a health summary of all agents",
     "output": "Agent Health Dashboard:\n\n1. finops-assistant: HEALTHY - Latency: 342ms avg, Error rate: 0.8%, Quality: 0.92, Cost: $176/day\n2. customer-support: WARNING - Latency: 1,100ms avg (SLA: 1,000ms), Error rate: 1.2%, Quality: 0.89, Cost: $407/day\n3. code-review: HEALTHY - Latency: 567ms avg, Error rate: 1.5%, Quality: 0.91, Cost: $224/day\n4. data-analysis: CRITICAL - Latency: 890ms avg, Error rate: 8.7%, Quality: 0.85 (drifting), Cost: $459/day (69% WoW increase)\n5. document-summarizer: WARNING - Latency: 2,100ms avg, Error rate: 0.5%, Quality: 0.78 (drift detected), Cost: $141/day\n\nOverall platform health: DEGRADED. 2 agents need attention."},
    {"input": "Recommend cost optimization actions",
     "output": "Top cost optimization recommendations:\n\n1. IMMEDIATE ($450/week savings): Migrate data-analysis-agent batch queries from PREMIUM_ALL_PURPOSE_COMPUTE to JOBS_COMPUTE. Current spend: $650/week on premium compute for scheduled batch jobs.\n\n2. MEDIUM-TERM ($300/week savings): Enable autoscaling on document-summarizer endpoint. Current fixed provisioning wastes 60% capacity during off-peak hours.\n\n3. OPTIMIZATION ($200/week savings): Rewrite top 5 expensive queries (see query optimization report). Full table scans account for 35% of SQL warehouse costs.\n\n4. MODEL SELECTION ($150/week savings): Route simple classification tasks from claude-sonnet to llama-3-3-70b. Quality difference is <2% for this use case.\n\nTotal potential savings: $1,100/week ($57,200/year)."},
]

training_rows = []
for pair in training_pairs:
    training_rows.append(Row(
        request=pair["input"],
        response=pair["output"],
        category=random.choice(["cost", "performance", "quality", "security", "anomaly", "optimization"]),
    ))

df_training = spark.createDataFrame(training_rows)
df_training.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.training_data")
print(f"Wrote {df_training.count()} training data records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Empty Results Tables

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, BooleanType

# Cost analysis results
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.cost_analysis_results (
    run_timestamp TIMESTAMP,
    analysis_type STRING,
    agent_name STRING,
    summary STRING,
    total_cost DOUBLE,
    cost_change_pct DOUBLE,
    recommendations STRING,
    raw_response STRING
)
""")

# Performance monitoring results
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.performance_results (
    run_timestamp TIMESTAMP,
    endpoint_name STRING,
    avg_latency_ms DOUBLE,
    p95_latency_ms DOUBLE,
    error_rate DOUBLE,
    sla_status STRING,
    summary STRING,
    recommendations STRING,
    raw_response STRING
)
""")

# Quality evaluation results
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.quality_results (
    run_timestamp TIMESTAMP,
    endpoint_name STRING,
    overall_score DOUBLE,
    drift_detected BOOLEAN,
    summary STRING,
    recommendations STRING,
    raw_response STRING
)
""")

# Query optimization results
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.query_optimization_results (
    run_timestamp TIMESTAMP,
    query_id STRING,
    query_text STRING,
    estimated_cost DOUBLE,
    duration_ms DOUBLE,
    optimization_suggestion STRING,
    priority STRING,
    raw_response STRING
)
""")

# Security audit results
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.security_results (
    run_timestamp TIMESTAMP,
    severity STRING,
    alert_type STRING,
    user_name STRING,
    description STRING,
    action_recommended STRING,
    raw_response STRING
)
""")

# Anomaly detection results
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.anomaly_results (
    run_timestamp TIMESTAMP,
    anomaly_type STRING,
    resource_name STRING,
    severity STRING,
    description STRING,
    metric_value DOUBLE,
    baseline_value DOUBLE,
    deviation_pct DOUBLE,
    raw_response STRING
)
""")

# Chat history
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.chat_history (
    timestamp TIMESTAMP,
    session_id STRING,
    user_message STRING,
    assistant_response STRING,
    model_used STRING
)
""")

print("All results tables created successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data

# COMMAND ----------

tables = [
    "serving_metrics", "billing_usage", "query_logs",
    "audit_logs", "quality_scores", "training_data"
]

for table in tables:
    count = spark.table(f"{CATALOG}.{SCHEMA}.{table}").count()
    print(f"  {table}: {count} rows")

print("\nTelemetry data generation complete!")
