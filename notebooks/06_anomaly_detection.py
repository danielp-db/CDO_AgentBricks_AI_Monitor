# Databricks notebook source

# MAGIC %md
# MAGIC # 06 - Anomaly Detection (Continuous / Every 10 min)
# MAGIC
# MAGIC Cross-dimensional anomaly detection using system tables:
# MAGIC - `system.billing.usage` for cost anomalies
# MAGIC - `system.ai_gateway.usage` for latency/error anomalies
# MAGIC - `system.access.audit` for security anomalies

# COMMAND ----------

dbutils.widgets.text("catalog", "att_log_anomaly_catalog", "Catalog")
dbutils.widgets.text("schema", "finops_monitor", "Schema")
dbutils.widgets.text("endpoint_name", "t2t-3ce36a81-endpoint", "Serving Endpoint")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistical Anomaly Detection on System Tables

# COMMAND ----------

from datetime import datetime, timedelta
import json

now = datetime.utcnow()

# Cost anomalies: daily DBU usage vs rolling average (system.billing.usage)
df_cost_anomalies = spark.sql("""
    WITH daily_usage AS (
        SELECT usage_date,
               sku_name,
               SUM(usage_quantity) as daily_dbus
        FROM system.billing.usage
        WHERE usage_date >= current_date() - INTERVAL 14 DAYS
        GROUP BY usage_date, sku_name
    ),
    stats AS (
        SELECT sku_name,
               AVG(daily_dbus) as avg_dbus,
               STDDEV(daily_dbus) as std_dbus
        FROM daily_usage
        WHERE usage_date < current_date()
        GROUP BY sku_name
        HAVING STDDEV(daily_dbus) > 0
    ),
    today AS (
        SELECT sku_name, daily_dbus
        FROM daily_usage
        WHERE usage_date = current_date()
    )
    SELECT t.sku_name as resource_name,
           t.daily_dbus as current_value,
           s.avg_dbus as baseline_value,
           s.std_dbus,
           (t.daily_dbus - s.avg_dbus) / s.std_dbus as z_score,
           ((t.daily_dbus - s.avg_dbus) / s.avg_dbus) * 100 as deviation_pct
    FROM today t JOIN stats s ON t.sku_name = s.sku_name
    WHERE ABS((t.daily_dbus - s.avg_dbus) / s.std_dbus) > 2
    ORDER BY ABS(z_score) DESC
""")

# Latency anomalies from ai_gateway: hourly avg vs 48h baseline
df_latency_anomalies = spark.sql("""
    WITH hourly AS (
        SELECT endpoint_name,
               DATE_TRUNC('HOUR', event_time) as hour,
               AVG(latency_ms) as hourly_latency,
               COUNT(*) as request_count
        FROM system.ai_gateway.usage
        WHERE event_time >= current_timestamp() - INTERVAL 48 HOURS
        GROUP BY endpoint_name, DATE_TRUNC('HOUR', event_time)
        HAVING COUNT(*) >= 5
    ),
    stats AS (
        SELECT endpoint_name,
               AVG(hourly_latency) as avg_latency,
               STDDEV(hourly_latency) as std_latency
        FROM hourly
        WHERE hour < current_timestamp() - INTERVAL 2 HOURS
        GROUP BY endpoint_name
        HAVING STDDEV(hourly_latency) > 0
    ),
    recent AS (
        SELECT endpoint_name, hourly_latency
        FROM hourly
        WHERE hour >= current_timestamp() - INTERVAL 2 HOURS
    )
    SELECT r.endpoint_name as resource_name,
           r.hourly_latency as current_value,
           s.avg_latency as baseline_value,
           (r.hourly_latency - s.avg_latency) / s.std_latency as z_score,
           ((r.hourly_latency - s.avg_latency) / s.avg_latency) * 100 as deviation_pct
    FROM recent r JOIN stats s ON r.endpoint_name = s.endpoint_name
    WHERE ABS((r.hourly_latency - s.avg_latency) / s.std_latency) > 2
""")

# Error rate spikes from ai_gateway
df_error_anomalies = spark.sql("""
    SELECT endpoint_name as resource_name,
           ROUND(SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as current_value,
           COUNT(*) as request_count
    FROM system.ai_gateway.usage
    WHERE event_time >= current_timestamp() - INTERVAL 1 HOUR
    GROUP BY endpoint_name
    HAVING SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 5
""")

# Audit anomalies: unusual volume of actions per user
df_audit_anomalies = spark.sql("""
    SELECT user_identity.email as resource_name,
           COUNT(*) as current_value,
           action_name
    FROM system.access.audit
    WHERE event_time >= current_timestamp() - INTERVAL 30 MINUTES
    GROUP BY user_identity.email, action_name
    HAVING COUNT(*) >= 100
    ORDER BY current_value DESC
""")

cost_anomalies = [row.asDict() for row in df_cost_anomalies.collect()]
latency_anomalies = [row.asDict() for row in df_latency_anomalies.collect()]
error_anomalies = [row.asDict() for row in df_error_anomalies.collect()]
audit_anomalies = [row.asDict() for row in df_audit_anomalies.collect()]

print(f"Anomalies detected: {len(cost_anomalies)} cost, {len(latency_anomalies)} latency, {len(error_anomalies)} error rate, {len(audit_anomalies)} audit")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call AgentBricks for Root Cause Analysis

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()

ENDPOINTS_TO_TRY = [ENDPOINT_NAME]

def call_finops_agent(prompt):
    system_msg = (
        "You are the AI FinOps Anomaly Detector. Analyze detected anomalies from Databricks system tables, "
        "correlate across dimensions (cost, latency, errors, audit), "
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


total_anomalies = len(cost_anomalies) + len(latency_anomalies) + len(error_anomalies) + len(audit_anomalies)

anomaly_prompt = f"""Analyze the following detected anomalies across Databricks system tables.

## Cost Anomalies from system.billing.usage (Z-score > 2):
{serialize_for_prompt(cost_anomalies) if cost_anomalies else "None detected"}

## Latency Anomalies from system.ai_gateway.usage (Z-score > 2):
{serialize_for_prompt(latency_anomalies) if latency_anomalies else "None detected"}

## Error Rate Spikes from system.ai_gateway.usage (>5%):
{serialize_for_prompt(error_anomalies) if error_anomalies else "None detected"}

## Audit Anomalies from system.access.audit (100+ actions in 30 min):
{serialize_for_prompt(audit_anomalies) if audit_anomalies else "None detected"}

Total anomalies detected: {total_anomalies}

For each anomaly:
1. Classify severity (CRITICAL/HIGH/MEDIUM/LOW)
2. Identify if anomalies correlate across systems
3. Provide root cause hypothesis
4. Recommend immediate action

Return JSON with:
- summary: Overall anomaly status
- anomalies: List of (anomaly_type, resource_name, severity, description, metric_value, baseline_value, deviation_pct)
- correlations: Any cross-system correlations found
- root_causes: Hypotheses
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
    for a in cost_anomalies:
        results.append(Row(
            run_timestamp=run_timestamp,
            anomaly_type="cost_spike",
            resource_name=str(a.get("resource_name", "unknown")),
            severity="HIGH" if abs(float(a.get("z_score", 0))) > 3 else "MEDIUM",
            description=f"Cost anomaly: z-score={float(a.get('z_score', 0)):.2f}",
            metric_value=float(a.get("current_value", 0)),
            baseline_value=float(a.get("baseline_value", 0)),
            deviation_pct=float(a.get("deviation_pct", 0)),
            raw_response=response_text or "",
        ))
    for a in latency_anomalies:
        results.append(Row(
            run_timestamp=run_timestamp,
            anomaly_type="latency_spike",
            resource_name=str(a.get("resource_name", "unknown")),
            severity="HIGH" if abs(float(a.get("z_score", 0))) > 3 else "MEDIUM",
            description=f"Latency anomaly: z-score={float(a.get('z_score', 0)):.2f}",
            metric_value=float(a.get("current_value", 0)),
            baseline_value=float(a.get("baseline_value", 0)),
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
