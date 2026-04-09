# Databricks notebook source

# MAGIC %md
# MAGIC # 06 - Anomaly Detection (Continuous / Every 10 min)
# MAGIC
# MAGIC Cross-dimensional anomaly detection: cost spikes, latency anomalies, quality degradation.
# MAGIC Uses AgentBricks FinOps Assistant for root cause analysis.

# COMMAND ----------

dbutils.widgets.text("catalog", "finops_monitor", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistical Anomaly Detection

# COMMAND ----------

from datetime import datetime, timedelta
import json

now = datetime.utcnow()

# Cost anomalies: current vs rolling average
df_cost_anomalies = spark.sql(f"""
    WITH daily_costs AS (
        SELECT DATE(usage_date) as date, agent_name, SUM(total_cost) as daily_cost
        FROM {CATALOG}.{SCHEMA}.billing_usage
        WHERE usage_date >= current_timestamp() - INTERVAL 14 DAYS
        GROUP BY DATE(usage_date), agent_name
    ),
    stats AS (
        SELECT agent_name,
               AVG(daily_cost) as avg_cost,
               STDDEV(daily_cost) as std_cost
        FROM daily_costs
        WHERE date < CURRENT_DATE()
        GROUP BY agent_name
    ),
    today AS (
        SELECT agent_name, daily_cost
        FROM daily_costs
        WHERE date = CURRENT_DATE()
    )
    SELECT t.agent_name,
           t.daily_cost as current_cost,
           s.avg_cost as baseline_cost,
           s.std_cost,
           (t.daily_cost - s.avg_cost) / NULLIF(s.std_cost, 0) as z_score,
           ((t.daily_cost - s.avg_cost) / NULLIF(s.avg_cost, 0)) * 100 as deviation_pct
    FROM today t JOIN stats s ON t.agent_name = s.agent_name
    WHERE ABS((t.daily_cost - s.avg_cost) / NULLIF(s.std_cost, 0)) > 2
    ORDER BY ABS(z_score) DESC
""")

# Latency anomalies
df_latency_anomalies = spark.sql(f"""
    WITH hourly AS (
        SELECT endpoint_name,
               DATE_TRUNC('HOUR', timestamp) as hour,
               AVG(avg_latency_ms) as hourly_latency
        FROM {CATALOG}.{SCHEMA}.serving_metrics
        WHERE timestamp >= current_timestamp() - INTERVAL 48 HOURS
        GROUP BY endpoint_name, DATE_TRUNC('HOUR', timestamp)
    ),
    stats AS (
        SELECT endpoint_name,
               AVG(hourly_latency) as avg_latency,
               STDDEV(hourly_latency) as std_latency
        FROM hourly
        WHERE hour < current_timestamp() - INTERVAL 2 HOURS
        GROUP BY endpoint_name
    ),
    recent AS (
        SELECT endpoint_name, hourly_latency
        FROM hourly
        WHERE hour >= current_timestamp() - INTERVAL 2 HOURS
    )
    SELECT r.endpoint_name,
           r.hourly_latency as current_latency,
           s.avg_latency as baseline_latency,
           (r.hourly_latency - s.avg_latency) / NULLIF(s.std_latency, 0) as z_score,
           ((r.hourly_latency - s.avg_latency) / NULLIF(s.avg_latency, 0)) * 100 as deviation_pct
    FROM recent r JOIN stats s ON r.endpoint_name = s.endpoint_name
    WHERE ABS((r.hourly_latency - s.avg_latency) / NULLIF(s.std_latency, 0)) > 2
""")

# Error rate spikes
df_error_anomalies = spark.sql(f"""
    SELECT endpoint_name,
           AVG(error_rate) as current_error_rate,
           MAX(error_rate) as max_error_rate
    FROM {CATALOG}.{SCHEMA}.serving_metrics
    WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
      AND error_rate > 0.05
    GROUP BY endpoint_name
""")

cost_anomalies = [row.asDict() for row in df_cost_anomalies.collect()]
latency_anomalies = [row.asDict() for row in df_latency_anomalies.collect()]
error_anomalies = [row.asDict() for row in df_error_anomalies.collect()]

print(f"Anomalies detected: {len(cost_anomalies)} cost, {len(latency_anomalies)} latency, {len(error_anomalies)} error rate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call AgentBricks for Root Cause Analysis

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
        "You are the AI FinOps Anomaly Detector. Analyze detected anomalies, "
        "correlate across dimensions (cost, latency, errors, quality), "
        "and provide root cause hypotheses. "
        "Return JSON with keys: summary, anomalies, correlations, root_causes, actions."
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


total_anomalies = len(cost_anomalies) + len(latency_anomalies) + len(error_anomalies)

anomaly_prompt = f"""Analyze the following detected anomalies across our GenAI platform.

## Cost Anomalies (Z-score > 2):
{serialize_for_prompt(cost_anomalies) if cost_anomalies else "None detected"}

## Latency Anomalies (Z-score > 2):
{serialize_for_prompt(latency_anomalies) if latency_anomalies else "None detected"}

## Error Rate Spikes (>5%):
{serialize_for_prompt(error_anomalies) if error_anomalies else "None detected"}

Total anomalies detected: {total_anomalies}

For each anomaly:
1. Classify severity (CRITICAL/HIGH/MEDIUM/LOW)
2. Identify if anomalies correlate (e.g., cost spike + latency spike on same endpoint)
3. Provide root cause hypothesis
4. Recommend immediate action

Return JSON with:
- summary: Overall anomaly status
- anomalies: List of (anomaly_type, resource_name, severity, description, metric_value, baseline_value, deviation_pct)
- correlations: Any cross-dimensional correlations found
- root_causes: Hypotheses for the anomalies
- actions: Prioritized remediation steps
"""

response_text, endpoint_used = call_finops_agent(anomaly_prompt)
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

    for anomaly in analysis.get("anomalies", []):
        results.append(Row(
            run_timestamp=run_timestamp,
            anomaly_type=anomaly.get("anomaly_type", "unknown"),
            resource_name=anomaly.get("resource_name", "unknown"),
            severity=anomaly.get("severity", "LOW"),
            description=anomaly.get("description", ""),
            metric_value=float(anomaly.get("metric_value", 0)),
            baseline_value=float(anomaly.get("baseline_value", 0)),
            deviation_pct=float(anomaly.get("deviation_pct", 0)),
            raw_response=response_text,
        ))

except Exception as e:
    print(f"Parse error: {e}")
    # Store raw anomalies from statistical detection
    for a in cost_anomalies:
        results.append(Row(
            run_timestamp=run_timestamp,
            anomaly_type="cost_spike",
            resource_name=a.get("agent_name", "unknown"),
            severity="HIGH" if abs(a.get("z_score", 0)) > 3 else "MEDIUM",
            description=f"Cost anomaly: z-score={a.get('z_score', 0):.2f}",
            metric_value=float(a.get("current_cost", 0)),
            baseline_value=float(a.get("baseline_cost", 0)),
            deviation_pct=float(a.get("deviation_pct", 0)),
            raw_response=response_text or "",
        ))
    for a in latency_anomalies:
        results.append(Row(
            run_timestamp=run_timestamp,
            anomaly_type="latency_spike",
            resource_name=a.get("endpoint_name", "unknown"),
            severity="HIGH" if abs(a.get("z_score", 0)) > 3 else "MEDIUM",
            description=f"Latency anomaly: z-score={a.get('z_score', 0):.2f}",
            metric_value=float(a.get("current_latency", 0)),
            baseline_value=float(a.get("baseline_latency", 0)),
            deviation_pct=float(a.get("deviation_pct", 0)),
            raw_response=response_text or "",
        ))

if results:
    df_results = spark.createDataFrame(results)
    df_results.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.anomaly_results")
    print(f"Stored {len(results)} anomaly detection results")
else:
    print("No anomalies detected in this run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly Summary

# COMMAND ----------

recent = spark.sql(f"""
    SELECT anomaly_type, severity, COUNT(*) as count
    FROM {CATALOG}.{SCHEMA}.anomaly_results
    WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM {CATALOG}.{SCHEMA}.anomaly_results)
    GROUP BY anomaly_type, severity
    ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END
""")

print("Anomaly Detection Summary:")
recent.show(truncate=False)
