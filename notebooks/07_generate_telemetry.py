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

dbutils.widgets.text("catalog", "att_log_anomaly_catalog", "Catalog")
dbutils.widgets.text("schema", "finops_monitor", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
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

training_pairs = []

# ── COST ANALYSIS (40+ pairs) ──────────────────────────────────────────────
_agents = ["customer-support", "code-review", "data-analysis", "document-summarizer", "finops-assistant", "security-scanner"]
_skus = ["FOUNDATION_MODEL_SERVING", "SERVERLESS_SQL", "JOBS_COMPUTE", "PREMIUM_ALL_PURPOSE_COMPUTE", "MODEL_SERVING", "STANDARD_ALL_PURPOSE_COMPUTE"]
_models = ["llama-3-3-70b", "claude-sonnet", "dbrx-instruct", "finops-custom-v1"]

for agent in _agents:
    weekly_cost = round(random.uniform(500, 4000), 2)
    daily_cost = round(weekly_cost / 7, 2)
    wow_change = round(random.uniform(-15, 70), 1)
    top_sku = random.choice(_skus)
    top_pct = round(random.uniform(30, 65), 1)

    training_pairs.append({"input": f"What is the total cost for {agent} this week?",
        "output": f"Based on the billing data, the {agent} agent consumed ${weekly_cost:,.2f} this week across all SKUs. The largest cost driver was {top_sku} at ${weekly_cost * top_pct / 100:,.2f} ({top_pct}%). This represents a {wow_change:+.1f}% change from last week.",
        "category": "cost"})

    training_pairs.append({"input": f"What is {agent}'s daily spend?",
        "output": f"The {agent} agent averages ${daily_cost:,.2f}/day over the past 7 days. Peak day was {'Monday' if random.random() > 0.5 else 'Thursday'} at ${daily_cost * random.uniform(1.2, 1.8):,.2f}. Lowest day was Sunday at ${daily_cost * random.uniform(0.3, 0.7):,.2f}. The daily trend shows {'an upward' if wow_change > 0 else 'a downward'} trajectory.",
        "category": "cost"})

    training_pairs.append({"input": f"Break down {agent} cost by SKU",
        "output": f"Cost breakdown for {agent} (last 7 days):\n" + "\n".join(
            [f"  - {sku}: ${random.uniform(50, 800):,.2f} ({random.uniform(5, 40):.1f}%)" for sku in random.sample(_skus, 4)]
        ) + f"\nTotal: ${weekly_cost:,.2f}. Recommendation: Review {top_sku} usage for optimization opportunities.",
        "category": "cost"})

training_pairs.append({"input": "Compare costs between this week and last week",
    "output": "Week-over-week cost comparison:\n| Agent | This Week | Last Week | Change |\n|-------|-----------|-----------|--------|\n| finops-assistant | $1,234 | $1,180 | +4.6% |\n| customer-support | $2,847 | $2,541 | +12.0% |\n| code-review | $1,567 | $1,623 | -3.5% |\n| data-analysis | $3,210 | $1,890 | +69.8% |\n| document-summarizer | $987 | $1,012 | -2.5% |\nTotal: $9,845 vs $8,246 (+19.4%). The data-analysis-agent increase is the primary driver due to a new batch job consuming 3x more DBUs.",
    "category": "cost"})

training_pairs.append({"input": "What is the token usage breakdown by model?",
    "output": "Token usage (last 7 days):\n| Model | Input Tokens | Output Tokens | Total | Cost |\n|-------|-------------|---------------|-------|------|\n| llama-3-3-70b | 12.4M | 4.2M | 16.6M | $332 |\n| claude-sonnet | 8.7M | 3.1M | 11.8M | $354 |\n| dbrx-instruct | 5.2M | 1.8M | 7.0M | $140 |\n| finops-custom-v1 | 2.1M | 0.9M | 3.0M | $60 |\nTotal: 38.4M tokens, $886. Claude-sonnet has highest cost-per-token ratio. Consider migrating low-complexity tasks to llama or dbrx.",
    "category": "cost"})

training_pairs.append({"input": "Which SKU is driving the most cost?",
    "output": "FOUNDATION_MODEL_SERVING is the top cost driver at $3,245/week (38% of total). Breakdown:\n1. FOUNDATION_MODEL_SERVING: $3,245 (38%)\n2. SERVERLESS_SQL: $2,112 (25%)\n3. PREMIUM_ALL_PURPOSE_COMPUTE: $1,456 (17%)\n4. JOBS_COMPUTE: $892 (10%)\n5. MODEL_SERVING: $534 (6%)\n6. STANDARD_ALL_PURPOSE_COMPUTE: $312 (4%)\nRecommendation: Audit foundation model calls — 22% are retry/duplicate requests that could be cached.",
    "category": "cost"})

training_pairs.append({"input": "What is the total platform cost this month?",
    "output": "Monthly platform cost summary (MTD):\n- Total: $34,567.89\n- Daily average: $1,152.26\n- Projected month-end: $34,567.89\n- Budget: $40,000 (86.4% utilized)\n- Top spending agent: data-analysis at $9,876 (28.6%)\n- Fastest growing cost: MODEL_SERVING (+45% MoM)\nStatus: Within budget. Monitor data-analysis agent growth rate.",
    "category": "cost"})

training_pairs.append({"input": "Recommend cost optimization actions",
    "output": "Top cost optimization recommendations:\n\n1. IMMEDIATE ($450/week savings): Migrate data-analysis-agent batch queries from PREMIUM_ALL_PURPOSE_COMPUTE to JOBS_COMPUTE. Current spend: $650/week on premium compute for scheduled batch jobs.\n\n2. MEDIUM-TERM ($300/week savings): Enable autoscaling on document-summarizer endpoint. Current fixed provisioning wastes 60% capacity during off-peak hours.\n\n3. OPTIMIZATION ($200/week savings): Rewrite top 5 expensive queries (see query optimization report). Full table scans account for 35% of SQL warehouse costs.\n\n4. MODEL SELECTION ($150/week savings): Route simple classification tasks from claude-sonnet to llama-3-3-70b. Quality difference is <2% for this use case.\n\nTotal potential savings: $1,100/week ($57,200/year).",
    "category": "cost"})

training_pairs.append({"input": "Show cost forecast for next 7 days",
    "output": "7-day cost forecast (linear projection from 14-day trend):\n- Day 1: $1,245 | Day 2: $1,267 | Day 3: $1,289 | Day 4: $1,312 | Day 5: $1,334 | Day 6: $1,098 (weekend) | Day 7: $987 (weekend)\nProjected total: $8,532 (vs $8,246 last week, +3.5%).\nRisk: data-analysis-agent trend suggests possible spike on Day 3 if batch job volume continues growing. Confidence interval: $7,900-$9,200.",
    "category": "cost"})

for model in _models:
    tok_in = round(random.uniform(2, 15), 1)
    tok_out = round(random.uniform(0.5, 5), 1)
    cost = round((tok_in + tok_out) * random.uniform(15, 35), 2)
    training_pairs.append({"input": f"How much are we spending on {model}?",
        "output": f"Model {model} usage (last 7 days):\n- Input tokens: {tok_in}M\n- Output tokens: {tok_out}M\n- Total tokens: {tok_in + tok_out}M\n- Estimated cost: ${cost:,.2f}\n- Avg tokens per request: {random.randint(500, 3000)}\n- Requests served: {random.randint(5000, 50000):,}\nCost efficiency: ${cost / (tok_in + tok_out):.2f} per million tokens.",
        "category": "cost"})

# ── PERFORMANCE MONITORING (40+ pairs) ─────────────────────────────────────
_endpoints = ["finops-assistant-agent", "customer-support-agent", "code-review-agent", "data-analysis-agent", "document-summarizer"]

for ep in _endpoints:
    avg_lat = round(random.uniform(200, 1200), 0)
    p95_lat = round(avg_lat * random.uniform(1.3, 2.0), 0)
    p99_lat = round(avg_lat * random.uniform(1.8, 3.0), 0)
    err_rate = round(random.uniform(0.2, 6.0), 2)
    sla_ok = avg_lat < 1000 and err_rate < 5.0

    training_pairs.append({"input": f"What is the current latency for {ep}?",
        "output": f"Latency metrics for {ep} (last hour):\n- Average: {avg_lat:.0f}ms\n- P50: {avg_lat * 0.8:.0f}ms\n- P95: {p95_lat:.0f}ms\n- P99: {p99_lat:.0f}ms\nSLA threshold: avg < 1000ms, p95 < 2000ms.\nStatus: {'WITHIN SLA' if sla_ok else 'SLA VIOLATION — avg latency exceeds 1000ms threshold'}.",
        "category": "performance"})

    training_pairs.append({"input": f"What is the error rate for {ep}?",
        "output": f"Error rate for {ep}: {err_rate}% (last hour), {err_rate * random.uniform(0.6, 1.2):.2f}% (24h average).\nError breakdown:\n- 4xx client errors: {random.randint(5, 50)}\n- 5xx server errors: {random.randint(0, 20)}\n- Timeout errors: {random.randint(0, 10)}\nSLA threshold: < 5%. Status: {'WITHIN SLA' if err_rate < 5 else 'VIOLATION — error rate exceeds 5% threshold'}.",
        "category": "performance"})

    training_pairs.append({"input": f"Show throughput for {ep}",
        "output": f"Throughput for {ep} (last 24 hours):\n- Total requests: {random.randint(5000, 50000):,}\n- Avg requests/min: {random.randint(5, 60)}\n- Peak requests/min: {random.randint(60, 200)} (at {random.randint(9, 17)}:00 UTC)\n- Lowest requests/min: {random.randint(1, 10)} (at 03:00 UTC)\nCapacity utilization: {random.randint(30, 85)}%. {'No scaling needed.' if random.random() > 0.4 else 'Consider scaling up during peak hours.'}",
        "category": "performance"})

    training_pairs.append({"input": f"Is {ep} meeting SLA?",
        "output": f"SLA compliance for {ep}:\n- Avg latency: {avg_lat:.0f}ms (threshold: 1000ms) — {'PASS' if avg_lat < 1000 else 'FAIL'}\n- P95 latency: {p95_lat:.0f}ms (threshold: 2000ms) — {'PASS' if p95_lat < 2000 else 'FAIL'}\n- Error rate: {err_rate}% (threshold: 5%) — {'PASS' if err_rate < 5 else 'FAIL'}\n- Uptime: {random.uniform(99.5, 99.99):.2f}% (threshold: 99.9%) — PASS\nOverall SLA status: {'COMPLIANT' if sla_ok else 'NON-COMPLIANT'}.",
        "category": "performance"})

training_pairs.append({"input": "Are there any latency SLA violations?",
    "output": "Yes, I detected 3 SLA violations in the last 24 hours:\n1. code-review-agent: p95 latency of 2,450ms exceeded the 2,000ms SLA at 14:23 UTC\n2. document-summarizer: p99 latency spike to 5,200ms at 09:15 UTC (anomaly detected)\n3. customer-support-agent: avg latency of 1,100ms exceeded 1,000ms SLA between 11:00-11:30 UTC\nRecommendation: Scale up the code-review-agent endpoint and investigate the document-summarizer spike.",
    "category": "performance"})

training_pairs.append({"input": "Which endpoint has the highest error rate?",
    "output": "code-review-agent has the highest error rate at 8.7% (last hour).\nRanking:\n1. code-review-agent: 8.7% (CRITICAL — exceeds 5% SLA)\n2. data-analysis-agent: 3.2%\n3. customer-support-agent: 1.8%\n4. document-summarizer: 0.5%\n5. finops-assistant-agent: 0.3%\nThe code-review-agent errors are primarily 5xx (server-side). Root cause: the backing model endpoint is returning intermittent OOM errors. Recommend increasing memory allocation.",
    "category": "performance"})

training_pairs.append({"input": "Show latency comparison across all endpoints",
    "output": "Endpoint latency comparison (last hour averages):\n| Endpoint | Avg | P50 | P95 | P99 | SLA |\n|----------|-----|-----|-----|-----|-----|\n| finops-assistant | 342ms | 280ms | 520ms | 890ms | OK |\n| customer-support | 1,100ms | 890ms | 1,650ms | 2,200ms | WARN |\n| code-review | 567ms | 450ms | 890ms | 1,340ms | OK |\n| data-analysis | 890ms | 720ms | 1,450ms | 2,100ms | OK |\n| document-summarizer | 2,100ms | 1,800ms | 3,200ms | 4,500ms | FAIL |\nSLA thresholds: avg < 1000ms, p95 < 2000ms.",
    "category": "performance"})

training_pairs.append({"input": "What is the overall platform uptime?",
    "output": "Platform uptime (last 30 days): 99.94%\nDowntime: 26 minutes total across 3 incidents.\n\nPer-endpoint uptime:\n- finops-assistant: 99.99%\n- customer-support: 99.97%\n- code-review: 99.91% (1 incident: 15 min outage on March 28)\n- data-analysis: 99.95%\n- document-summarizer: 99.88% (2 incidents)\n\nSLA target: 99.9%. Status: COMPLIANT overall. document-summarizer is at risk.",
    "category": "performance"})

# ── QUALITY EVALUATION (35+ pairs) ─────────────────────────────────────────
for ep in _endpoints:
    rel = round(random.uniform(0.7, 0.95), 3)
    faith = round(random.uniform(0.72, 0.96), 3)
    safe = round(random.uniform(0.88, 1.0), 3)
    coh = round(random.uniform(0.75, 0.95), 3)
    overall = round((rel + faith + safe + coh) / 4, 3)
    drift = random.random() < 0.3
    drift_pct = round(random.uniform(8, 18), 1) if drift else 0

    training_pairs.append({"input": f"What are the quality scores for {ep}?",
        "output": f"Quality scores for {ep} (latest evaluation):\n- Relevance: {rel}\n- Faithfulness: {faith}\n- Safety: {safe}\n- Coherence: {coh}\n- Overall: {overall}\nBenchmark: overall >= 0.80 (acceptable), >= 0.90 (good).\nStatus: {'GOOD' if overall >= 0.9 else 'ACCEPTABLE' if overall >= 0.8 else 'BELOW THRESHOLD'}. {'Drift detected: overall score declined ' + str(drift_pct) + '% over 7 days.' if drift else 'No drift detected.'}",
        "category": "quality"})

    training_pairs.append({"input": f"Has {ep} quality degraded?",
        "output": f"Quality trend for {ep} (14-day window):\n- Current overall: {overall}\n- 7-day ago: {overall + drift_pct / 100 if drift else overall + random.uniform(-0.02, 0.02):.3f}\n- 14-day ago: {overall + drift_pct / 50 if drift else overall + random.uniform(-0.03, 0.03):.3f}\n{'DRIFT DETECTED: ' + str(drift_pct) + '% decline in overall score. Primary driver: relevance score dropped from ' + str(rel + 0.12) + ' to ' + str(rel) + '. Recommend retraining with updated evaluation data.' if drift else 'No significant drift. Scores are within normal variance (< 3% fluctuation).'}",
        "category": "quality"})

training_pairs.append({"input": "Which agents have quality drift?",
    "output": "Quality drift detected in 2 agents over the past 14 days:\n1. document-summarizer: relevance score dropped from 0.91 to 0.78 (-14.3%). Drift started ~5 days ago.\n2. data-analysis-agent: faithfulness score declined from 0.88 to 0.82 (-6.8%). Gradual decline over 10 days.\nAll other agents maintain stable quality scores within normal variance. Recommend retraining document-summarizer with updated evaluation data.",
    "category": "quality"})

training_pairs.append({"input": "Show quality trends for the last month",
    "output": "30-day quality overview:\n- Stable agents (3): finops-assistant (0.92 avg), customer-support (0.89), code-review (0.91)\n- Degrading agents (2): document-summarizer (0.91 -> 0.78, -14.3%), data-analysis (0.88 -> 0.82, -6.8%)\n- Safety scores: All agents above 0.90 threshold. No safety concerns.\n- Evaluation volume: 4,567 evaluations across all agents.\nAction items: Prioritize document-summarizer retraining. Schedule data-analysis review for next week.",
    "category": "quality"})

training_pairs.append({"input": "What is the safety score across agents?",
    "output": "Safety scores (latest evaluation):\n| Agent | Safety Score | Status |\n|-------|-------------|--------|\n| finops-assistant | 0.98 | PASS |\n| customer-support | 0.96 | PASS |\n| code-review | 0.97 | PASS |\n| data-analysis | 0.94 | PASS |\n| document-summarizer | 0.91 | PASS |\nAll agents above the 0.90 safety threshold. No safety incidents in the past 30 days.",
    "category": "quality"})

training_pairs.append({"input": "How many evaluations were run this week?",
    "output": "Evaluation summary (this week):\n- Total evaluations: 1,245\n- By agent: finops-assistant (267), customer-support (312), code-review (198), data-analysis (256), document-summarizer (212)\n- Judge model: databricks-meta-llama-3-3-70b-instruct\n- Average evaluation latency: 2.3 seconds\n- Pass rate: 89.4% (scores above 0.80 threshold)",
    "category": "quality"})

training_pairs.append({"input": "What is the faithfulness score trend?",
    "output": "Faithfulness score trend (14 days):\n- finops-assistant: 0.93 -> 0.92 (stable)\n- customer-support: 0.87 -> 0.88 (slight improvement)\n- code-review: 0.90 -> 0.91 (stable)\n- data-analysis: 0.88 -> 0.82 (DECLINING -6.8%)\n- document-summarizer: 0.86 -> 0.81 (DECLINING -5.8%)\nFaithfulness measures how well responses align with source data. Declining agents may be hallucinating more frequently. Recommend reviewing prompt templates and adding retrieval grounding.",
    "category": "quality"})

# ── QUERY OPTIMIZATION (30+ pairs) ─────────────────────────────────────────
_query_patterns = [
    ("SELECT * FROM serving_metrics WHERE date > '2025-01-01'", "full_table_scan", "Add partition pruning on date column: WHERE date > '2025-01-01' AND date_partition >= '2025-01-01'"),
    ("SELECT * FROM billing_usage bu JOIN serving_metrics sm ON 1=1", "cross_join", "Add proper join predicate: ON bu.agent_name = sm.endpoint_name AND bu.usage_date = DATE(sm.timestamp)"),
    ("SELECT agent_name, SUM(total_cost) FROM billing_usage GROUP BY agent_name", "missing_filter", "Add date filter to limit scan: WHERE usage_date >= current_date() - INTERVAL 7 DAYS"),
    ("SELECT COUNT(*) FROM serving_metrics", "full_count", "Use approximate count for large tables: SELECT approx_count_distinct(id) or query metadata tables"),
    ("INSERT INTO results SELECT * FROM staging_table", "unbounded_insert", "Add WHERE clause to limit insert scope and consider MERGE for upserts"),
    ("SELECT *, ROW_NUMBER() OVER (PARTITION BY agent_name ORDER BY total_cost DESC) FROM billing_usage", "expensive_window", "Materialize the window function result as a temp view. Add date filter to reduce partition size"),
    ("MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET *", "broad_merge", "Use specific columns in SET clause instead of SET *. Add date predicate to narrow merge scope"),
    ("WITH cte AS (SELECT * FROM serving_metrics) SELECT * FROM cte WHERE timestamp > now() - INTERVAL 1 HOUR", "cte_full_scan", "Move timestamp filter into CTE: WITH cte AS (SELECT * FROM serving_metrics WHERE timestamp > now() - INTERVAL 1 HOUR)"),
]

for query, issue, fix in _query_patterns:
    dur = round(random.uniform(5000, 60000), 0)
    bytes_gb = round(dur * random.uniform(0.01, 0.05), 1)
    cost = round(dur * 0.0001, 2)

    training_pairs.append({"input": f"How can I optimize this query: {query[:80]}...?",
        "output": f"Query analysis:\n- Issue: {issue.replace('_', ' ')}\n- Duration: {dur:.0f}ms\n- Data scanned: {bytes_gb}GB\n- Estimated cost: ${cost}\n\nOptimization: {fix}\n\nExpected improvement: {random.randint(40, 80)}% reduction in scan size, {random.randint(30, 70)}% faster execution.",
        "category": "optimization"})

    training_pairs.append({"input": f"Why is this query slow: {query[:60]}?",
        "output": f"This query is slow because of: {issue.replace('_', ' ')}.\n\nDetails:\n- Current execution time: {dur:.0f}ms\n- Bytes scanned: {bytes_gb}GB\n- The query {'scans the entire table without partition pruning' if 'scan' in issue else 'uses an inefficient pattern that prevents predicate pushdown'}.\n\nFix: {fix}\nPriority: {'HIGH' if cost > 3 else 'MEDIUM'}.",
        "category": "optimization"})

training_pairs.append({"input": "Show me the most expensive queries today",
    "output": "Top 5 most expensive queries today:\n1. Full table scan on serving_metrics (32.4s, 1.2TB scanned) - $12.84 - Run by analytics-team\n2. Cross-join between serving_metrics and billing_usage (28.1s, 890GB) - $11.24 - Run by ml-pipeline\n3. Unoptimized GROUP BY on billing_usage (15.7s, 456GB) - $6.28 - Run by service-account-finops\n4. Recursive CTE on query_logs (12.3s, 234GB) - $4.92 - Run by daniel.perez\n5. MERGE INTO with missing predicate (9.8s, 178GB) - $3.92 - Run by admin\nTotal estimated cost: $39.20. Recommendations: Add partition pruning to query #1, rewrite #2 join condition.",
    "category": "optimization"})

training_pairs.append({"input": "What queries are failing most often?",
    "output": "Top failing queries (last 24h):\n1. INSERT INTO results SELECT * FROM staging (12 failures) - User: ml-pipeline - Error: DELTA_TABLE_NOT_FOUND\n2. SELECT * FROM system.billing.usage (8 failures) - User: service-account-finops - Error: PERMISSION_DENIED\n3. MERGE INTO target USING source (5 failures) - User: analytics-team - Error: CONCURRENT_WRITE_CONFLICT\n4. CREATE TABLE AS SELECT (3 failures) - User: admin - Error: TABLE_ALREADY_EXISTS\nTotal failed queries: 42 out of 1,234 (3.4% failure rate).",
    "category": "optimization"})

training_pairs.append({"input": "Which users run the most expensive queries?",
    "output": "Top query spenders (last 7 days):\n1. analytics-team@databricks.com: $156.78 (342 queries, avg $0.46/query)\n2. ml-pipeline@databricks.com: $98.45 (567 queries, avg $0.17/query)\n3. service-account-finops@databricks.com: $67.23 (234 queries, avg $0.29/query)\n4. daniel.perez@databricks.com: $34.56 (123 queries, avg $0.28/query)\n5. admin@databricks.com: $23.12 (89 queries, avg $0.26/query)\nRecommendation: analytics-team has high per-query cost — review their top 10 queries for optimization.",
    "category": "optimization"})

# ── SECURITY AUDITING (35+ pairs) ──────────────────────────────────────────
_sec_users = ["admin@databricks.com", "service-account-finops@databricks.com", "ml-pipeline@databricks.com", "analytics-team@databricks.com", "unknown-user@external.com"]
_ips = ["10.42.88.12", "10.0.1.55", "192.168.1.100", "10.255.0.1", "172.16.4.88"]

for user in _sec_users:
    failed = random.randint(0, 20)
    training_pairs.append({"input": f"Are there failed login attempts for {user}?",
        "output": f"Failed login analysis for {user} (last 24 hours):\n- Failed attempts: {failed}\n- Source IPs: {', '.join(random.sample(_ips, min(3, len(_ips))))}\n- Time window: {random.randint(1, 12)} hour span\n{'- ALERT: ' + str(failed) + ' failed attempts exceeds threshold (10). Pattern suggests brute force attack. Recommend: temporarily lock account and investigate source IPs.' if failed > 10 else '- Status: Within normal range. No action required.'}",
        "category": "security"})

    training_pairs.append({"input": f"What actions has {user} performed recently?",
        "output": f"Activity log for {user} (last 6 hours):\n- workspace.access (LOGIN): {random.randint(1, 5)} times\n- sql.executeStatement: {random.randint(5, 50)} times\n- unityCatalog.getTable: {random.randint(10, 100)} times\n- endpoints.query: {random.randint(0, 30)} times\n{'- permissions.changePermission: ' + str(random.randint(1, 5)) + ' times — REQUIRES REVIEW' if random.random() < 0.3 else ''}\n{'- tokens.create: ' + str(random.randint(3, 10)) + ' times — UNUSUAL VOLUME' if random.random() < 0.2 else ''}\nRisk assessment: {'ELEVATED — permission changes and token creation detected' if random.random() < 0.25 else 'NORMAL — routine operational activity'}.",
        "category": "security"})

training_pairs.append({"input": "Are there any security concerns?",
    "output": "Security audit flagged 4 concerns:\n1. HIGH: 15 failed login attempts from IP 10.42.88.12 for user admin@databricks.com in the last hour - possible brute force\n2. MEDIUM: tokens.create called 8 times by service-account-finops - unusual token generation pattern\n3. MEDIUM: permissions.changePermission on 3 Unity Catalog tables by analytics-team - verify authorization\n4. LOW: secrets.getSecret accessed 47 times by ml-pipeline - high frequency but from known service account\nImmediate action recommended for concern #1. Verify concerns #2 and #3 with respective teams.",
    "category": "security"})

training_pairs.append({"input": "Show permission changes in the last 24 hours",
    "output": "Permission changes detected (last 24 hours):\n1. analytics-team changed permissions on catalog.schema.billing_usage — added READ to external-group (14:23 UTC)\n2. admin changed permissions on catalog.schema.audit_logs — removed ALL from temp-service-account (11:05 UTC)\n3. analytics-team changed permissions on catalog.schema.serving_metrics — added WRITE to ml-pipeline (16:45 UTC)\n4. service-account-finops changed permissions on catalog.schema.training_data — added READ to new-team (09:30 UTC)\n\nRisk assessment: Change #1 grants access to billing data to an external group — verify this is authorized. Change #3 grants write access to serving metrics — unusual.",
    "category": "security"})

training_pairs.append({"input": "Any unusual API token activity?",
    "output": "Token activity analysis (last 24 hours):\n- Total tokens created: 14\n- Normal baseline: 3-5 per day\n- ALERT: service-account-finops created 8 tokens in 2 hours (09:00-11:00 UTC)\n- Tokens by user: service-account-finops (8), admin (3), ml-pipeline (2), analytics-team (1)\n- Token types: PAT (10), OAuth (4)\n\nSeverity: MEDIUM. The spike from service-account-finops is unusual. Possible causes: automated rotation script malfunction, credential leak response, or unauthorized activity. Recommend: verify with the service account owner.",
    "category": "security"})

training_pairs.append({"input": "What is the current security risk score?",
    "output": "Overall security risk score: 62/100 (MEDIUM)\n\nBreakdown:\n- Authentication: 70/100 (brute force attempts detected)\n- Authorization: 55/100 (permission changes need review)\n- Token management: 65/100 (unusual token creation volume)\n- Data access: 50/100 (normal patterns)\n- Network: 45/100 (all access from known IP ranges)\n\nTop 3 risk factors:\n1. 15 failed login attempts for admin account\n2. 8 tokens created by single service account\n3. 4 permission changes to sensitive tables\n\nRecommendation: Address authentication risk immediately. Review permission changes within 4 hours.",
    "category": "security"})

training_pairs.append({"input": "Show suspicious IP addresses",
    "output": "Suspicious IP analysis (last 48 hours):\n| IP Address | Failed Logins | Successful Logins | Actions | Risk |\n|-----------|--------------|-------------------|---------|------|\n| 10.42.88.12 | 15 | 0 | 0 | HIGH |\n| 172.16.4.88 | 8 | 2 | 12 | MEDIUM |\n| 10.255.0.1 | 5 | 1 | 3 | LOW |\n\n10.42.88.12 has zero successful logins with 15 failures — classic brute force pattern. 172.16.4.88 has mixed activity suggesting compromised credentials with partial success. Recommend: block 10.42.88.12 immediately, monitor 172.16.4.88.",
    "category": "security"})

# ── ANOMALY DETECTION (35+ pairs) ──────────────────────────────────────────
_anomaly_types = ["cost_spike", "latency_spike", "error_rate_spike", "quality_degradation", "throughput_drop", "token_surge"]

for agent in _agents:
    for atype in random.sample(_anomaly_types, 3):
        metric_val = round(random.uniform(100, 5000), 2)
        baseline = round(metric_val / random.uniform(2, 8), 2)
        dev_pct = round((metric_val - baseline) / baseline * 100, 1)

        training_pairs.append({"input": f"What {atype.replace('_', ' ')} anomalies are there for {agent}?",
            "output": f"Anomaly detected for {agent} — {atype.replace('_', ' ')}:\n- Current value: {metric_val}\n- Baseline (7-day avg): {baseline}\n- Deviation: {dev_pct:+.1f}%\n- Z-score: {random.uniform(2.1, 5.5):.1f}\n- First detected: {random.randint(1, 12)} hours ago\n- Severity: {'CRITICAL' if dev_pct > 200 else 'HIGH' if dev_pct > 100 else 'MEDIUM'}\n\nPossible root cause: {'Recent deployment changed model configuration' if 'cost' in atype else 'Increased traffic volume exceeding endpoint capacity' if 'latency' in atype else 'Upstream data quality issue affecting agent responses'}.\nRecommended action: {'Roll back latest deployment and investigate' if dev_pct > 200 else 'Scale endpoint and monitor for 1 hour'}.",
            "category": "anomaly"})

training_pairs.append({"input": "What anomalies were detected in the last 6 hours?",
    "output": "Anomaly Detection Summary (last 6 hours):\n- COST: Spike of 340% for data-analysis-agent MODEL_SERVING costs at 08:00 UTC. Estimated excess: $456.78\n- LATENCY: document-summarizer p99 latency jumped from 1,200ms baseline to 5,200ms at 09:15 UTC (4.3x increase)\n- ERROR RATE: code-review-agent error rate increased from 1.2% to 8.7% between 10:00-10:30 UTC\n- QUALITY: No quality anomalies detected\n- SECURITY: Brute force pattern detected (see security alerts)\nRoot cause hypothesis: The cost and latency spikes correlate with a deployment at 07:45 UTC. Recommend reviewing recent model updates.",
    "category": "anomaly"})

training_pairs.append({"input": "Are any anomalies correlated?",
    "output": "Correlation analysis across detected anomalies:\n\n1. STRONG CORRELATION: data-analysis-agent cost spike (340%) + latency increase (180%) — both started within 15 minutes of each other at ~08:00 UTC. Likely same root cause: increased request volume or model change.\n\n2. MODERATE CORRELATION: code-review-agent error rate spike + customer-support-agent latency increase — error cascading from shared infrastructure.\n\n3. NO CORRELATION: document-summarizer quality drift is a gradual trend unrelated to acute anomalies.\n\nRoot cause hypothesis: A deployment at 07:45 UTC affected the shared serving infrastructure, causing cascading cost and performance impacts.",
    "category": "anomaly"})

training_pairs.append({"input": "Show anomaly history for the past week",
    "output": "Anomaly history (past 7 days):\n| Date | Type | Resource | Severity | Resolved |\n|------|------|----------|----------|----------|\n| Today | cost_spike | data-analysis | CRITICAL | No |\n| Today | latency_spike | document-summarizer | HIGH | No |\n| Today | error_rate | code-review | HIGH | No |\n| Yesterday | cost_spike | customer-support | MEDIUM | Yes |\n| 3 days ago | latency_spike | code-review | MEDIUM | Yes |\n| 5 days ago | quality_drift | document-summarizer | MEDIUM | No |\n| 6 days ago | throughput_drop | finops-assistant | LOW | Yes |\n\nTotal: 7 anomalies. 3 unresolved. Trend: anomaly frequency increasing — 3 today vs 1-2/day baseline.",
    "category": "anomaly"})

# ── GENERAL / HEALTH SUMMARY (15+ pairs) ──────────────────────────────────
training_pairs.append({"input": "Give me a health summary of all agents",
    "output": "Agent Health Dashboard:\n\n1. finops-assistant: HEALTHY - Latency: 342ms avg, Error rate: 0.8%, Quality: 0.92, Cost: $176/day\n2. customer-support: WARNING - Latency: 1,100ms avg (SLA: 1,000ms), Error rate: 1.2%, Quality: 0.89, Cost: $407/day\n3. code-review: HEALTHY - Latency: 567ms avg, Error rate: 1.5%, Quality: 0.91, Cost: $224/day\n4. data-analysis: CRITICAL - Latency: 890ms avg, Error rate: 8.7%, Quality: 0.85 (drifting), Cost: $459/day (69% WoW increase)\n5. document-summarizer: WARNING - Latency: 2,100ms avg, Error rate: 0.5%, Quality: 0.78 (drift detected), Cost: $141/day\n\nOverall platform health: DEGRADED. 2 agents need attention.",
    "category": "general"})

training_pairs.append({"input": "What is the overall platform status?",
    "output": "Platform Status: DEGRADED\n\nSummary:\n- Active agents: 5/5 (all running)\n- SLA compliance: 3/5 agents within SLA\n- Security risk: MEDIUM (62/100)\n- Active anomalies: 3 (1 CRITICAL, 2 HIGH)\n- Weekly cost: $9,845 (+19.4% WoW)\n- Quality: 3 stable, 2 degrading\n\nImmediate attention needed:\n1. data-analysis-agent: cost spike + error rate spike\n2. document-summarizer: latency SLA violation + quality drift\n3. Security: brute force login attempts on admin account\n\nNext review: scheduled in 15 minutes.",
    "category": "general"})

training_pairs.append({"input": "What happened in the last hour?",
    "output": "Last hour activity summary:\n\n- Requests served: 12,456 across all endpoints\n- Average latency: 687ms (within SLA)\n- Errors: 234 (1.9% error rate)\n- Cost incurred: $142.56\n- New anomalies: 1 (cost spike on data-analysis-agent)\n- Security events: 15 failed logins from 10.42.88.12 (HIGH alert)\n- Quality evaluations: 45 completed (avg score: 0.87)\n- Queries executed: 234 (5 flagged as expensive)\n\nNotable events:\n- 08:00 UTC: data-analysis cost spike detected (340% above baseline)\n- 08:15 UTC: Security alert triggered for brute force pattern",
    "category": "general"})

training_pairs.append({"input": "Generate an executive summary report",
    "output": "Executive Summary — GenAI Platform FinOps Report\n\nPeriod: Last 7 days\n\nCost: $9,845 total (+19.4% WoW). Primary driver: data-analysis-agent batch job increase. On track for $42,000/month (budget: $40,000 — 5% over).\n\nPerformance: 99.94% uptime. 3 SLA violations detected. document-summarizer requires scaling.\n\nQuality: 3/5 agents stable. 2 agents showing quality drift (document-summarizer, data-analysis). Retraining recommended.\n\nSecurity: Risk score 62/100 (MEDIUM). 1 brute force attempt detected and blocked. 4 permission changes require review.\n\nAnomalies: 7 detected (3 unresolved). Increasing trend — investigate shared infrastructure.\n\nKey Recommendations:\n1. Investigate data-analysis cost increase ($1,320 excess)\n2. Scale document-summarizer endpoint\n3. Retrain document-summarizer model\n4. Block suspicious IP 10.42.88.12\n5. Review analytics-team permission changes\n\nEstimated savings opportunity: $1,100/week ($57,200/year).",
    "category": "general"})

training_pairs.append({"input": "What should I focus on right now?",
    "output": "Priority action list (ranked by urgency):\n\n1. CRITICAL — Block IP 10.42.88.12 (15 failed login attempts, active brute force)\n2. CRITICAL — Investigate data-analysis-agent cost spike (340% above baseline, $456 excess)\n3. HIGH — Fix code-review-agent error rate (8.7%, exceeding 5% SLA)\n4. HIGH — Scale document-summarizer endpoint (2,100ms avg latency, SLA: 1,000ms)\n5. MEDIUM — Review analytics-team permission changes on sensitive tables\n6. MEDIUM — Schedule document-summarizer retraining (quality drift -14.3%)\n7. LOW — Optimize top 5 expensive queries ($39/day potential savings)\n\nEstimated resolution time: items 1-2 (~30 min), items 3-4 (~2 hours), items 5-7 (~1 day).",
    "category": "general"})

training_pairs.append({"input": "How does today compare to yesterday?",
    "output": "Day-over-day comparison:\n| Metric | Today | Yesterday | Change |\n|--------|-------|-----------|--------|\n| Total requests | 28,456 | 31,234 | -8.9% |\n| Avg latency | 687ms | 542ms | +26.8% |\n| Error rate | 1.9% | 0.8% | +137.5% |\n| Total cost | $1,407 | $1,178 | +19.4% |\n| Security alerts | 4 | 1 | +300% |\n| Anomalies | 3 | 1 | +200% |\n| Avg quality | 0.87 | 0.89 | -2.2% |\n\nToday is significantly worse across all dimensions. Root cause: deployment at 07:45 UTC triggered cascading issues.",
    "category": "general"})

training_pairs.append({"input": "What are the top risks to the platform?",
    "output": "Top platform risks (ranked):\n\n1. COST OVERRUN RISK (HIGH): data-analysis-agent growing at 69% WoW. At current rate, will exceed monthly budget in 8 days.\n\n2. AVAILABILITY RISK (HIGH): document-summarizer latency at 2.1x SLA threshold. One more traffic spike could cause cascading timeouts.\n\n3. SECURITY RISK (MEDIUM): Active brute force pattern. If admin credentials are compromised, attacker gains full workspace access.\n\n4. QUALITY RISK (MEDIUM): Two agents drifting. Customer-facing responses from document-summarizer may be noticeably degraded.\n\n5. OPERATIONAL RISK (LOW): Anomaly frequency increasing (3x daily average). May indicate systemic infrastructure issue.\n\nMitigation plan: Address risks 1-3 within 4 hours. Schedule risk 4-5 review for tomorrow.",
    "category": "general"})

training_pairs.append({"input": "Show me the dashboard overview",
    "output": "FinOps Dashboard Overview:\n\n--- COST ---\nWeekly: $9,845 | Daily avg: $1,406 | WoW: +19.4% | Budget: 86% used\n\n--- PERFORMANCE ---\nAvg latency: 687ms | P95: 1,450ms | Error rate: 1.9% | Uptime: 99.94%\nSLA violations: 3 (customer-support, code-review, document-summarizer)\n\n--- QUALITY ---\nAvg overall: 0.87 | Drift detected: 2 agents | Safety: all > 0.90\n\n--- SECURITY ---\nRisk score: 62/100 | Active alerts: 4 | Failed logins: 28 | Perm changes: 4\n\n--- ANOMALIES ---\nActive: 3 (1 CRITICAL, 2 HIGH) | Resolved today: 0 | 7-day total: 7\n\nStatus: DEGRADED — immediate attention required on 3 items.",
    "category": "general"})

# ── ADDITIONAL VARIATIONS (75+ pairs to reach 200+) ────────────────────────

# Cost: time-range variations per agent
for agent in _agents:
    for period, days, mult in [("today", 1, 0.14), ("this month", 30, 4.3), ("last 3 days", 3, 0.43)]:
        base = round(random.uniform(500, 4000) * mult, 2)
        training_pairs.append({"input": f"How much did {agent} cost {period}?",
            "output": f"Cost for {agent} ({period}): ${base:,.2f}.\nTop SKU: {random.choice(_skus)} at ${base * random.uniform(0.3, 0.6):,.2f}.\nTokens consumed: {random.randint(100, 5000)}K input, {random.randint(50, 2000)}K output.\n{'Spend is elevated compared to the rolling average.' if random.random() < 0.3 else 'Spend is within expected range.'}",
            "category": "cost"})

# Performance: endpoint-specific deep dives
for ep in _endpoints:
    rps = random.randint(5, 80)
    training_pairs.append({"input": f"What is the request rate for {ep}?",
        "output": f"{ep} request rate:\n- Current: {rps} req/min\n- 1h avg: {rps + random.randint(-10, 10)} req/min\n- 24h avg: {rps + random.randint(-20, 5)} req/min\n- Peak today: {rps * random.randint(2, 4)} req/min at {random.randint(9, 17)}:00 UTC\nCapacity headroom: {random.randint(20, 60)}%.",
        "category": "performance"})

    training_pairs.append({"input": f"Show error breakdown for {ep}",
        "output": f"Error breakdown for {ep} (last 24h):\n- Total errors: {random.randint(10, 200)}\n- 400 Bad Request: {random.randint(2, 30)} ({random.randint(5, 25)}%)\n- 429 Rate Limited: {random.randint(0, 50)} ({random.randint(0, 30)}%)\n- 500 Internal Error: {random.randint(5, 60)} ({random.randint(20, 50)}%)\n- 503 Unavailable: {random.randint(0, 20)} ({random.randint(0, 15)}%)\n- 504 Timeout: {random.randint(0, 15)} ({random.randint(0, 10)}%)\nMost errors are 500s — indicates backend model issues, not client problems.",
        "category": "performance"})

# Quality: metric-specific questions
for metric, desc in [("relevance", "how well responses match the user query"), ("faithfulness", "accuracy relative to source data"), ("coherence", "logical consistency of responses"), ("safety", "absence of harmful content")]:
    training_pairs.append({"input": f"What is the {metric} score across all agents?",
        "output": f"{metric.title()} scores ({desc}):\n" + "\n".join(
            [f"- {ep}: {random.uniform(0.75, 0.98):.3f}" for ep in _endpoints]
        ) + f"\nPlatform average: {random.uniform(0.82, 0.93):.3f}. Threshold: 0.80.\n{'All agents above threshold.' if random.random() > 0.3 else 'document-summarizer trending below threshold — recommend review.'}",
        "category": "quality"})

# Quality: retraining questions
for ep in _endpoints:
    training_pairs.append({"input": f"Should we retrain {ep}?",
        "output": f"Retraining assessment for {ep}:\n- Current overall score: {random.uniform(0.75, 0.95):.3f}\n- 7-day trend: {random.choice(['-2.1%', '-8.4%', '+0.5%', '-12.3%', '+1.2%'])}\n- Drift detected: {'Yes' if random.random() < 0.4 else 'No'}\n- Last retrained: {random.randint(7, 45)} days ago\n- Evaluation volume: {random.randint(100, 500)} samples\n\nRecommendation: {'YES — quality decline exceeds 10% threshold. Priority: HIGH.' if random.random() < 0.4 else 'NOT YET — scores within acceptable range. Re-evaluate in 7 days.'}",
        "category": "quality"})

# Security: action-specific queries
for action, desc in [("tokens.create", "API token creation"), ("permissions.changePermission", "permission modifications"), ("secrets.getSecret", "secret access"), ("clusters.create", "cluster provisioning"), ("endpoints.create", "endpoint creation")]:
    training_pairs.append({"input": f"Show recent {action} events",
        "output": f"Recent {action} events ({desc}) — last 24 hours:\n" + "\n".join(
            [f"  {i+1}. {random.choice(_sec_users)} at {random.randint(0,23):02d}:{random.randint(0,59):02d} UTC from {random.choice(_ips)} via {random.choice(['API', 'UI', 'CLI'])}" for i in range(random.randint(3, 7))]
        ) + f"\n\nTotal events: {random.randint(3, 20)}. {'ALERT: volume exceeds daily baseline by 2x. Investigate.' if random.random() < 0.3 else 'Volume within normal range.'}",
        "category": "security"})

# Anomaly: cross-agent summaries
for atype in _anomaly_types:
    training_pairs.append({"input": f"Are there any {atype.replace('_', ' ')} anomalies right now?",
        "output": f"Current {atype.replace('_', ' ')} anomalies:\n" + (
            "\n".join([f"- {random.choice(_agents)}: deviation {random.uniform(50, 400):.0f}% above baseline, severity {'CRITICAL' if random.random() < 0.2 else 'HIGH' if random.random() < 0.5 else 'MEDIUM'}, detected {random.randint(1, 6)}h ago"
                for _ in range(random.randint(1, 3))])
            if random.random() < 0.7
            else "No active anomalies of this type."
        ) + f"\n\nBaseline window: 7-day rolling average. Detection method: Z-score > 2.0.",
        "category": "anomaly"})

# Anomaly: root cause analysis
for agent in _agents:
    training_pairs.append({"input": f"What is the root cause of {agent} anomalies?",
        "output": f"Root cause analysis for {agent}:\n\nDetected anomalies:\n- {random.choice(_anomaly_types).replace('_', ' ')}: {random.uniform(100, 500):.0f}% deviation\n- {random.choice(_anomaly_types).replace('_', ' ')}: {random.uniform(50, 200):.0f}% deviation\n\nTimeline:\n- {random.randint(1, 12)}h ago: First anomaly detected\n- {random.randint(0, 6)}h ago: Second anomaly correlated\n\nHypotheses (ranked by likelihood):\n1. Recent model update changed inference behavior (70% confidence)\n2. Upstream data quality degradation (20% confidence)\n3. Infrastructure capacity constraint (10% confidence)\n\nRecommended investigation: Check deployment logs for the past 24h, compare model versions, review input data distribution.",
        "category": "anomaly"})

# General: comparative and trend questions
for period in ["this week vs last week", "today vs 7-day average", "weekday vs weekend"]:
    training_pairs.append({"input": f"Compare platform metrics {period}",
        "output": f"Platform comparison ({period}):\n- Total cost: ${random.uniform(5000, 12000):,.0f} vs ${random.uniform(5000, 12000):,.0f} ({random.uniform(-20, 30):+.1f}%)\n- Avg latency: {random.randint(400, 900)}ms vs {random.randint(400, 900)}ms\n- Error rate: {random.uniform(0.5, 3):.1f}% vs {random.uniform(0.5, 3):.1f}%\n- Requests: {random.randint(50000, 200000):,} vs {random.randint(50000, 200000):,}\n- Anomalies: {random.randint(1, 8)} vs {random.randint(1, 8)}\n- Avg quality: {random.uniform(0.83, 0.93):.2f} vs {random.uniform(0.83, 0.93):.2f}",
        "category": "general"})

# General: additional common questions
training_pairs.append({"input": "How many requests did we serve today?",
    "output": f"Total requests served today: {random.randint(20000, 60000):,}\n\nBy endpoint:\n" + "\n".join([f"- {ep}: {random.randint(3000, 15000):,}" for ep in _endpoints]) + f"\n\nPeak hour: {random.randint(9,17)}:00 UTC ({random.randint(3000, 8000):,} requests)\nLowest hour: 03:00 UTC ({random.randint(200, 800):,} requests)",
    "category": "general"})

training_pairs.append({"input": "What is our cost per request?",
    "output": f"Cost per request analysis:\n| Agent | Cost/Request | Requests | Total Cost |\n|-------|-------------|----------|------------|\n" + "\n".join([f"| {a} | ${random.uniform(0.01, 0.15):.3f} | {random.randint(2000, 15000):,} | ${random.uniform(100, 800):,.2f} |" for a in _agents]) + f"\n\nPlatform average: ${random.uniform(0.03, 0.08):.3f}/request.\nMost efficient: finops-assistant. Least efficient: data-analysis (complex multi-step reasoning).",
    "category": "cost"})

training_pairs.append({"input": "What is the average response quality this week?",
    "output": f"Average response quality (this week):\n- Overall: {random.uniform(0.84, 0.92):.3f}\n- Relevance: {random.uniform(0.82, 0.93):.3f}\n- Faithfulness: {random.uniform(0.83, 0.94):.3f}\n- Safety: {random.uniform(0.92, 0.99):.3f}\n- Coherence: {random.uniform(0.84, 0.93):.3f}\n\nCompared to last week: {random.choice(['+1.2%', '-2.3%', '+0.5%', '-0.8%', 'unchanged'])}\nEvaluations completed: {random.randint(500, 2000)}",
    "category": "quality"})

training_pairs.append({"input": "List all active alerts",
    "output": f"Active alerts ({random.randint(3, 8)} total):\n\n1. CRITICAL: data-analysis-agent cost spike 340% above baseline (2h ago)\n2. HIGH: code-review-agent error rate 8.7% exceeds 5% SLA (1h ago)\n3. HIGH: document-summarizer latency 2,100ms exceeds 1,000ms SLA (3h ago)\n4. HIGH: 15 failed login attempts on admin account (45 min ago)\n5. MEDIUM: document-summarizer quality drift -14.3% over 7 days\n6. MEDIUM: analytics-team unauthorized permission changes\n7. LOW: finops-assistant throughput dropped 15% (normal weekend pattern)\n\nAcknowledged: 0. Auto-resolved: 0.",
    "category": "general"})

training_pairs.append({"input": "Show me cost trends for MODEL_SERVING",
    "output": f"MODEL_SERVING cost trend (14 days):\n" + "\n".join([f"- Day {i+1}: ${random.uniform(80, 350):,.2f}" for i in range(7)]) + f"\n...\nWeekly total: ${random.uniform(800, 2400):,.2f}\nWoW change: {random.uniform(-10, 45):+.1f}%\nProjection: ${random.uniform(900, 2800):,.2f} next week\n\nDriven primarily by data-analysis-agent ({random.randint(40, 60)}% of MODEL_SERVING spend).",
    "category": "cost"})

training_pairs.append({"input": "How do I reduce latency on document-summarizer?",
    "output": "Latency reduction plan for document-summarizer:\n\nCurrent state: 2,100ms avg (SLA: 1,000ms)\n\nRecommendations:\n1. IMMEDIATE: Scale endpoint from 1 to 3 replicas — expected 60% latency reduction\n2. SHORT-TERM: Enable request batching — group similar requests to reduce per-request overhead\n3. MEDIUM-TERM: Switch to a smaller model for simple summarization tasks (llama-3-3-70b → dbrx-instruct)\n4. OPTIMIZATION: Cache frequent document summaries — 35% of requests are duplicates\n5. ARCHITECTURE: Add a queue with priority routing for time-sensitive requests\n\nExpected outcome: avg latency < 800ms within 48 hours of implementing steps 1-2.",
    "category": "performance"})

training_pairs.append({"input": "What permissions does analytics-team have?",
    "output": "analytics-team@databricks.com permissions audit:\n\nUnity Catalog:\n- att_log_anomaly_catalog.finops_monitor.billing_usage: SELECT, MODIFY\n- att_log_anomaly_catalog.finops_monitor.serving_metrics: SELECT, MODIFY (recently added)\n- att_log_anomaly_catalog.finops_monitor.query_logs: SELECT\n\nWorkspace:\n- Can create clusters: Yes\n- Can create jobs: Yes\n- Can access SQL warehouses: Yes (wh-finops-001)\n\nRecent changes:\n- Added MODIFY on serving_metrics (16:45 UTC today) — REQUIRES REVIEW\n- Added READ for external-group on billing_usage (14:23 UTC today) — REQUIRES REVIEW\n\nRisk: MEDIUM — MODIFY access to serving_metrics is unusual for analytics team.",
    "category": "security"})

training_pairs.append({"input": "Summarize all anomalies by severity",
    "output": "Anomaly summary by severity (last 24 hours):\n\nCRITICAL (1):\n- data-analysis-agent cost spike: 340% above baseline, $456.78 excess\n\nHIGH (3):\n- code-review-agent error rate: 8.7% (SLA: 5%)\n- document-summarizer latency: 2,100ms (SLA: 1,000ms)\n- admin account brute force: 15 failed login attempts\n\nMEDIUM (2):\n- document-summarizer quality drift: -14.3% over 7 days\n- analytics-team permission changes: 3 tables modified\n\nLOW (1):\n- finops-assistant throughput: 15% decrease (weekend pattern)\n\nTotal: 7 anomalies. Resolution rate: 0% (none resolved yet).",
    "category": "anomaly"})

training_pairs.append({"input": "What is the estimated monthly bill at current rate?",
    "output": f"Monthly cost projection:\n\nCurrent daily average: $1,406\nProjected monthly total: $42,180\nBudget: $40,000\nVariance: +$2,180 (5.5% over budget)\n\nBreakdown by agent:\n" + "\n".join([f"- {a}: ${random.uniform(3000, 12000):,.0f}/month" for a in _agents]) + "\n\nRisk: data-analysis-agent alone could push budget 12% over if growth continues.\nSavings opportunity: $1,100/week identified in optimization recommendations.\nWith optimizations applied: projected $37,400/month (6.5% under budget).",
    "category": "cost"})

# General: agent-specific summary
for agent in _agents:
    training_pairs.append({"input": f"Give me a full status report for {agent}",
        "output": f"Status report for {agent}:\n\n--- COST ---\nDaily: ${random.uniform(100, 500):,.2f} | Weekly: ${random.uniform(700, 3500):,.2f} | WoW: {random.uniform(-15, 50):+.1f}%\n\n--- PERFORMANCE ---\nLatency: {random.randint(200, 1200)}ms avg | Error rate: {random.uniform(0.2, 5):.1f}% | Throughput: {random.randint(5, 60)} req/min\nSLA: {'COMPLIANT' if random.random() > 0.3 else 'VIOLATION'}\n\n--- QUALITY ---\nOverall: {random.uniform(0.75, 0.95):.3f} | Drift: {'Yes' if random.random() < 0.3 else 'No'}\n\n--- SECURITY ---\nRecent events: {random.randint(10, 200)} | Alerts: {random.randint(0, 3)}\n\nHealth: {'HEALTHY' if random.random() > 0.4 else 'WARNING' if random.random() > 0.3 else 'CRITICAL'}",
        "category": "general"})

# Assign categories to any pairs missing them
training_rows = []
for pair in training_pairs:
    category = pair.get("category", random.choice(["cost", "performance", "quality", "security", "anomaly", "optimization", "general"]))
    training_rows.append(Row(
        request=pair["input"],
        response=pair["output"],
        category=category,
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
