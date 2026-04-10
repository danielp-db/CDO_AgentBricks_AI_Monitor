# Databricks notebook source

# MAGIC %md
# MAGIC # 03 - Quality Evaluation (Daily)
# MAGIC
# MAGIC Derives quality signals from `system.ai_gateway.usage` (error rates, latency distribution)
# MAGIC and combines with MLflow evaluation scores. Uses AgentBricks FinOps Assistant to assess trends.

# COMMAND ----------

dbutils.widgets.text("catalog", "att_log_anomaly_catalog", "Catalog")
dbutils.widgets.text("schema", "finops_monitor", "Schema")
dbutils.widgets.text("endpoint_name", "t2t-3ce36a81-endpoint", "Serving Endpoint")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Quality Signals from system.ai_gateway.usage

# COMMAND ----------

from datetime import datetime, timedelta
import json

now = datetime.utcnow()

# Per-endpoint quality proxy: error rates, latency consistency, token efficiency
df_recent = spark.sql("""
    SELECT endpoint_name,
           destination_name as model_name,
           COUNT(*) as total_requests,
           ROUND(SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) as error_rate_pct,
           ROUND(SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) as server_error_pct,
           AVG(latency_ms) as avg_latency,
           STDDEV(latency_ms) as latency_stddev,
           AVG(output_tokens) as avg_output_tokens,
           STDDEV(output_tokens) as output_token_stddev,
           AVG(CASE WHEN output_tokens > 0 THEN input_tokens / output_tokens ELSE NULL END) as input_output_ratio
    FROM system.ai_gateway.usage
    WHERE event_time >= current_timestamp() - INTERVAL 7 DAYS
    GROUP BY endpoint_name, destination_name
    HAVING COUNT(*) >= 10
""")

# Weekly trend: daily error rates per endpoint
df_daily_quality = spark.sql("""
    SELECT DATE(event_time) as date,
           endpoint_name,
           COUNT(*) as requests,
           ROUND(SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) as error_rate_pct,
           AVG(latency_ms) as avg_latency,
           PERCENTILE(latency_ms, 0.95) as p95_latency
    FROM system.ai_gateway.usage
    WHERE event_time >= current_timestamp() - INTERVAL 14 DAYS
    GROUP BY DATE(event_time), endpoint_name
    ORDER BY date
""")

# Previous week baseline
df_baseline = spark.sql("""
    SELECT endpoint_name,
           ROUND(SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) as baseline_error_rate,
           AVG(latency_ms) as baseline_latency,
           AVG(output_tokens) as baseline_output_tokens
    FROM system.ai_gateway.usage
    WHERE event_time >= current_timestamp() - INTERVAL 14 DAYS
      AND event_time < current_timestamp() - INTERVAL 7 DAYS
    GROUP BY endpoint_name
""")

recent_data = [row.asDict() for row in df_recent.collect()]
daily_data = [row.asDict() for row in df_daily_quality.collect()]
baseline_data = [row.asDict() for row in df_baseline.collect()]

print(f"Quality data: {len(recent_data)} endpoints, {len(daily_data)} daily records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call AgentBricks for Quality Analysis

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()

ENDPOINTS_TO_TRY = [ENDPOINT_NAME]

def call_finops_agent(prompt):
    system_msg = (
        "You are the AI FinOps Quality Evaluator. Analyze serving endpoint quality signals "
        "(error rates, latency consistency, token efficiency) to detect degradation patterns. "
        "Return JSON with keys: summary, drift_alerts, quality_report, recommendations."
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
        except Exception:
            continue
    return None, None


def serialize_for_prompt(data):
    def default_handler(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return str(obj)
    return json.dumps(data, indent=2, default=default_handler)


quality_prompt = f"""Analyze quality signals for our GenAI agent endpoints using system.ai_gateway.usage data.

## Quality Indicators:
- Error rate: < 2% (good), < 5% (acceptable), > 5% (degraded)
- Latency consistency: low stddev relative to mean = stable quality
- Output token consistency: high variance may indicate inconsistent responses
- Drift alert: >50% increase in error rate or latency week-over-week

## Current Week Quality Signals:
{serialize_for_prompt(recent_data)}

## Previous Week Baseline:
{serialize_for_prompt(baseline_data)}

## Daily Trend (14 days):
{serialize_for_prompt(daily_data[:30])}

Return JSON with:
- summary: Overall quality status
- drift_alerts: Endpoints with quality degradation (endpoint_name, metric, current, baseline, change_pct)
- quality_report: Per-endpoint assessment (endpoint_name, status, overall_score, notes)
  - Compute overall_score as: 1.0 - (error_rate_pct/100) - (latency_stddev/avg_latency * 0.1), clamped to [0,1]
- recommendations: Improvement actions
"""

response_text, endpoint_used = call_finops_agent(quality_prompt)
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

    for report in analysis.get("quality_report", []):
        ep_name = report.get("endpoint_name", "unknown")
        drift = any(d.get("endpoint_name") == ep_name for d in analysis.get("drift_alerts", []))

        results.append(Row(
            run_timestamp=run_timestamp,
            endpoint_name=ep_name,
            overall_score=float(report.get("overall_score", 0)),
            drift_detected=drift,
            summary=report.get("notes", report.get("status", "")),
            recommendations=json.dumps(analysis.get("recommendations", [])),
            raw_response=response_text,
        ))

except Exception as e:
    print(f"Parse error: {e}")
    for metric in recent_data:
        err_pct = float(metric.get("error_rate_pct", 0))
        score = max(0, min(1.0, 1.0 - err_pct / 100.0))
        results.append(Row(
            run_timestamp=run_timestamp,
            endpoint_name=metric.get("endpoint_name", "unknown"),
            overall_score=score,
            drift_detected=False,
            summary=response_text[:500] if response_text else "No response",
            recommendations="",
            raw_response=response_text or "",
        ))

if results:
    df_results = spark.createDataFrame(results)
    df_results.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.quality_results")
    print(f"Stored {len(results)} quality evaluation results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drift Summary

# COMMAND ----------

drifting = spark.sql(f"""
    SELECT endpoint_name, overall_score, drift_detected, summary
    FROM {CATALOG}.{SCHEMA}.quality_results
    WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM {CATALOG}.{SCHEMA}.quality_results)
      AND drift_detected = true
""")

if drifting.count() > 0:
    print("Quality Drift Detected:")
    drifting.show(truncate=False)
else:
    print("No quality drift detected. All endpoints stable.")
