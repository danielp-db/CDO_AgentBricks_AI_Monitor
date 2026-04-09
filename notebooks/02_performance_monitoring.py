# Databricks notebook source

# MAGIC %md
# MAGIC # 02 - Performance Monitoring (Every 15 min)
# MAGIC
# MAGIC Latency, throughput, error rate tracking with SLA validation.
# MAGIC Calls the AgentBricks FinOps Assistant to evaluate performance against SLAs.

# COMMAND ----------

dbutils.widgets.text("catalog", "finops_monitor", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Performance Metrics

# COMMAND ----------

from datetime import datetime, timedelta
import json

now = datetime.utcnow()

# Last hour of metrics per endpoint
df_recent = spark.sql(f"""
    SELECT endpoint_name,
           model_name,
           AVG(avg_latency_ms) as avg_latency,
           AVG(p95_latency_ms) as avg_p95_latency,
           AVG(p99_latency_ms) as avg_p99_latency,
           SUM(request_count) as total_requests,
           SUM(error_count) as total_errors,
           AVG(error_rate) as avg_error_rate,
           SUM(total_tokens) as total_tokens,
           MAX(CASE WHEN is_anomaly THEN 1 ELSE 0 END) as has_anomaly
    FROM {CATALOG}.{SCHEMA}.serving_metrics
    WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
    GROUP BY endpoint_name, model_name
    ORDER BY avg_latency DESC
""")

# SLA thresholds
SLA_CONFIG = {
    "avg_latency_ms": 1000,
    "p95_latency_ms": 2000,
    "error_rate_pct": 5.0,
}

recent_data = [row.asDict() for row in df_recent.collect()]

# Historical baseline (last 24 hours)
df_baseline = spark.sql(f"""
    SELECT endpoint_name,
           AVG(avg_latency_ms) as baseline_latency,
           AVG(error_rate) as baseline_error_rate,
           AVG(request_count) as baseline_throughput
    FROM {CATALOG}.{SCHEMA}.serving_metrics
    WHERE timestamp >= current_timestamp() - INTERVAL 24 HOURS
      AND timestamp < current_timestamp() - INTERVAL 1 HOUR
    GROUP BY endpoint_name
""")

baseline_data = {row.endpoint_name: row.asDict() for row in df_baseline.collect()}

print(f"Collected metrics for {len(recent_data)} endpoint-model pairs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call AgentBricks for Performance Analysis

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()

ENDPOINTS_TO_TRY = [
    "finops-assistant-agent",
    "databricks-meta-llama-3-3-70b-instruct",
]

def call_finops_agent(prompt):
    system_msg = (
        "You are the AI FinOps Performance Monitor. Analyze serving endpoint metrics, "
        "validate against SLA thresholds, and flag violations. "
        "Return JSON with keys: summary, sla_violations, endpoint_health, recommendations."
    )
    messages = [
        ChatMessage(role=ChatMessageRole.SYSTEM, content=system_msg),
        ChatMessage(role=ChatMessageRole.USER, content=prompt),
    ]
    for endpoint in ENDPOINTS_TO_TRY:
        try:
            response = w.serving_endpoints.query(
                name=endpoint, messages=messages, max_tokens=4000, temperature=0.3,
            )
            return response.choices[0].message.content, endpoint
        except Exception as e:
            print(f"  Endpoint {endpoint} failed: {e}")
    return None, None


def serialize_for_prompt(data):
    def default_handler(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return str(obj)
    return json.dumps(data, indent=2, default=default_handler)


perf_prompt = f"""Analyze the following serving endpoint performance metrics and check for SLA violations.

## SLA Thresholds:
- Average Latency: < {SLA_CONFIG['avg_latency_ms']}ms
- P95 Latency: < {SLA_CONFIG['p95_latency_ms']}ms
- Error Rate: < {SLA_CONFIG['error_rate_pct']}%

## Current Metrics (Last Hour):
{serialize_for_prompt(recent_data)}

## 24-Hour Baseline:
{serialize_for_prompt(baseline_data)}

Return JSON with:
- summary: Overall performance status
- sla_violations: List of violations (endpoint_name, metric, current_value, threshold, severity)
- endpoint_health: Per-endpoint status (endpoint_name, status [HEALTHY/WARNING/CRITICAL], details)
- recommendations: Ranked list of actions
"""

response_text, endpoint_used = call_finops_agent(perf_prompt)
print(f"Response from: {endpoint_used}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Results

# COMMAND ----------

from pyspark.sql import Row

run_timestamp = datetime.utcnow()
results = []

try:
    json_text = response_text
    if "```json" in json_text:
        json_text = json_text.split("```json")[1].split("```")[0]
    elif "```" in json_text:
        json_text = json_text.split("```")[1].split("```")[0]

    analysis = json.loads(json_text)

    for ep_health in analysis.get("endpoint_health", []):
        ep_name = ep_health.get("endpoint_name", "unknown")
        metric = next((m for m in recent_data if m.get("endpoint_name") == ep_name), {})

        results.append(Row(
            run_timestamp=run_timestamp,
            endpoint_name=ep_name,
            avg_latency_ms=float(metric.get("avg_latency", 0)),
            p95_latency_ms=float(metric.get("avg_p95_latency", 0)),
            error_rate=float(metric.get("avg_error_rate", 0)),
            sla_status=ep_health.get("status", "UNKNOWN"),
            summary=ep_health.get("details", ""),
            recommendations=json.dumps(analysis.get("recommendations", [])),
            raw_response=response_text,
        ))

except Exception as e:
    print(f"Parse error: {e}")
    for metric in recent_data:
        sla_status = "HEALTHY"
        if metric.get("avg_latency", 0) > SLA_CONFIG["avg_latency_ms"]:
            sla_status = "WARNING"
        if metric.get("avg_error_rate", 0) > SLA_CONFIG["error_rate_pct"] / 100:
            sla_status = "CRITICAL"

        results.append(Row(
            run_timestamp=run_timestamp,
            endpoint_name=metric.get("endpoint_name", "unknown"),
            avg_latency_ms=float(metric.get("avg_latency", 0)),
            p95_latency_ms=float(metric.get("avg_p95_latency", 0)),
            error_rate=float(metric.get("avg_error_rate", 0)),
            sla_status=sla_status,
            summary=response_text[:500] if response_text else "No response",
            recommendations="",
            raw_response=response_text or "",
        ))

if results:
    df_results = spark.createDataFrame(results)
    df_results.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.performance_results")
    print(f"Stored {len(results)} performance results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check SLA Violations

# COMMAND ----------

violations = spark.sql(f"""
    SELECT endpoint_name, sla_status, avg_latency_ms, p95_latency_ms, error_rate
    FROM {CATALOG}.{SCHEMA}.performance_results
    WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM {CATALOG}.{SCHEMA}.performance_results)
      AND sla_status != 'HEALTHY'
    ORDER BY CASE sla_status WHEN 'CRITICAL' THEN 1 WHEN 'WARNING' THEN 2 ELSE 3 END
""")

if violations.count() > 0:
    print("SLA Violations Detected:")
    violations.show(truncate=False)
else:
    print("All endpoints within SLA thresholds.")
