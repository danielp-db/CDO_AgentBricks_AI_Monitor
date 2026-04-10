# Databricks notebook source

# MAGIC %md
# MAGIC # 02 - Performance Monitoring (Every 15 min)
# MAGIC
# MAGIC Latency, throughput, error rate tracking with SLA validation.
# MAGIC Uses `system.ai_gateway.usage` for serving endpoint metrics.

# COMMAND ----------

dbutils.widgets.text("catalog", "att_log_anomaly_catalog", "Catalog")
dbutils.widgets.text("schema", "finops_monitor", "Schema")
dbutils.widgets.text("endpoint_name", "t2t-3ce36a81-endpoint", "Serving Endpoint")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Performance Metrics from system.ai_gateway.usage

# COMMAND ----------

from datetime import datetime, timedelta
import json

now = datetime.utcnow()

# SLA thresholds
SLA_CONFIG = {
    "avg_latency_ms": 1000,
    "p95_latency_ms": 2000,
    "error_rate_pct": 5.0,
}

# Last hour metrics per endpoint
df_recent = spark.sql("""
    SELECT endpoint_name,
           destination_name as model_name,
           COUNT(*) as request_count,
           AVG(latency_ms) as avg_latency,
           PERCENTILE(latency_ms, 0.5) as p50_latency,
           PERCENTILE(latency_ms, 0.95) as p95_latency,
           PERCENTILE(latency_ms, 0.99) as p99_latency,
           SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) as error_count,
           ROUND(SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as error_rate_pct,
           SUM(total_tokens) as total_tokens,
           SUM(input_tokens) as input_tokens,
           SUM(output_tokens) as output_tokens
    FROM system.ai_gateway.usage
    WHERE event_time >= current_timestamp() - INTERVAL 1 HOUR
    GROUP BY endpoint_name, destination_name
    ORDER BY avg_latency DESC
""")

# 24h baseline per endpoint
df_baseline = spark.sql("""
    SELECT endpoint_name,
           AVG(latency_ms) as baseline_latency,
           ROUND(SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as baseline_error_rate,
           COUNT(*) / 24.0 as baseline_throughput_per_hour
    FROM system.ai_gateway.usage
    WHERE event_time >= current_timestamp() - INTERVAL 24 HOURS
      AND event_time < current_timestamp() - INTERVAL 1 HOUR
    GROUP BY endpoint_name
""")

recent_data = [row.asDict() for row in df_recent.collect()]
baseline_data = {row.endpoint_name: row.asDict() for row in df_baseline.collect()}

print(f"Collected metrics for {len(recent_data)} endpoint-model pairs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call AgentBricks for Performance Analysis

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()

ENDPOINTS_TO_TRY = [ENDPOINT_NAME]

def call_finops_agent(prompt):
    system_msg = (
        "You are the AI FinOps Performance Monitor. Analyze serving endpoint metrics from system.ai_gateway.usage, "
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


perf_prompt = f"""Analyze the following serving endpoint performance metrics from system.ai_gateway.usage and check for SLA violations.

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
            p95_latency_ms=float(metric.get("p95_latency", 0)),
            error_rate=float(metric.get("error_rate_pct", 0)) / 100.0,
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
        if metric.get("error_rate_pct", 0) > SLA_CONFIG["error_rate_pct"]:
            sla_status = "CRITICAL"

        results.append(Row(
            run_timestamp=run_timestamp,
            endpoint_name=metric.get("endpoint_name", "unknown"),
            avg_latency_ms=float(metric.get("avg_latency", 0)),
            p95_latency_ms=float(metric.get("p95_latency", 0)),
            error_rate=float(metric.get("error_rate_pct", 0)) / 100.0,
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
