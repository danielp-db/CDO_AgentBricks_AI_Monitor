# Databricks notebook source

# MAGIC %md
# MAGIC # 05 - Security Auditing (Every 5 min)
# MAGIC
# MAGIC Unauthorized access detection, permission drift monitoring.
# MAGIC Uses AgentBricks FinOps Assistant for threat analysis.

# COMMAND ----------

dbutils.widgets.text("catalog", "finops_monitor", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Security Data

# COMMAND ----------

from datetime import datetime, timedelta
import json

now = datetime.utcnow()

# Recent failed logins (last 30 min)
df_failed_logins = spark.sql(f"""
    SELECT user_name, ip_address, COUNT(*) as attempt_count,
           MIN(timestamp) as first_attempt, MAX(timestamp) as last_attempt
    FROM {CATALOG}.{SCHEMA}.audit_logs
    WHERE timestamp >= current_timestamp() - INTERVAL 30 MINUTES
      AND action = 'workspace.access'
      AND status = 'FAILURE'
    GROUP BY user_name, ip_address
    HAVING COUNT(*) >= 3
    ORDER BY attempt_count DESC
""")

# Permission changes (last hour)
df_permission_changes = spark.sql(f"""
    SELECT user_name, resource_id, timestamp, source, ip_address
    FROM {CATALOG}.{SCHEMA}.audit_logs
    WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
      AND action = 'permissions.changePermission'
    ORDER BY timestamp DESC
""")

# Unusual token creation
df_tokens = spark.sql(f"""
    SELECT user_name, COUNT(*) as token_count,
           MIN(timestamp) as first_created, MAX(timestamp) as last_created
    FROM {CATALOG}.{SCHEMA}.audit_logs
    WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
      AND action = 'tokens.create'
    GROUP BY user_name
    HAVING COUNT(*) >= 3
""")

# Suspicious activity summary
df_suspicious = spark.sql(f"""
    SELECT action, action_type, user_name, source, COUNT(*) as count
    FROM {CATALOG}.{SCHEMA}.audit_logs
    WHERE timestamp >= current_timestamp() - INTERVAL 30 MINUTES
      AND is_suspicious = true
    GROUP BY action, action_type, user_name, source
    ORDER BY count DESC
""")

failed_logins = [row.asDict() for row in df_failed_logins.collect()]
perm_changes = [row.asDict() for row in df_permission_changes.collect()]
token_events = [row.asDict() for row in df_tokens.collect()]
suspicious = [row.asDict() for row in df_suspicious.collect()]

print(f"Security data: {len(failed_logins)} failed login patterns, {len(perm_changes)} permission changes, {len(token_events)} token anomalies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call AgentBricks for Security Analysis

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
        "You are the AI FinOps Security Auditor. Analyze audit logs for security threats "
        "including brute force attacks, unauthorized access, permission drift, and suspicious "
        "activity. Classify severity: CRITICAL, HIGH, MEDIUM, LOW. "
        "Return JSON with keys: summary, alerts, risk_score, immediate_actions."
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


security_prompt = f"""Analyze the following security audit data and identify threats.

## Failed Login Attempts (Last 30 min, 3+ attempts):
{serialize_for_prompt(failed_logins)}

## Permission Changes (Last Hour):
{serialize_for_prompt(perm_changes)}

## Unusual Token Creation (3+ in 1 hour):
{serialize_for_prompt(token_events)}

## Flagged Suspicious Activity:
{serialize_for_prompt(suspicious)}

For each finding, classify severity:
- CRITICAL: Active attack or confirmed breach (brute force with 10+ attempts, mass permission changes)
- HIGH: Likely malicious activity (repeated failed logins, unusual admin actions)
- MEDIUM: Requires investigation (permission changes, token creation spikes)
- LOW: Informational (unusual patterns from known accounts)

Return JSON with:
- summary: Overall security posture
- alerts: List of (severity, alert_type, user_name, description, action_recommended)
- risk_score: 0-100 overall risk score
- immediate_actions: List of actions needed right now
"""

response_text, endpoint_used = call_finops_agent(security_prompt)
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

    for alert in analysis.get("alerts", []):
        results.append(Row(
            run_timestamp=run_timestamp,
            severity=alert.get("severity", "LOW"),
            alert_type=alert.get("alert_type", "unknown"),
            user_name=alert.get("user_name", ""),
            description=alert.get("description", ""),
            action_recommended=alert.get("action_recommended", ""),
            raw_response=response_text,
        ))

except Exception as e:
    print(f"Parse error: {e}")
    # Store summary result
    if failed_logins or suspicious:
        results.append(Row(
            run_timestamp=run_timestamp,
            severity="MEDIUM",
            alert_type="security_scan",
            user_name="system",
            description=response_text[:500] if response_text else "Scan completed",
            action_recommended="Review audit logs manually",
            raw_response=response_text or "",
        ))

if results:
    df_results = spark.createDataFrame(results)
    df_results.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.security_results")
    print(f"Stored {len(results)} security alerts")
else:
    print("No security alerts to store. All clear.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Critical Alerts

# COMMAND ----------

critical = spark.sql(f"""
    SELECT severity, alert_type, user_name, description
    FROM {CATALOG}.{SCHEMA}.security_results
    WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM {CATALOG}.{SCHEMA}.security_results)
      AND severity IN ('CRITICAL', 'HIGH')
""")

if critical.count() > 0:
    print("CRITICAL/HIGH Security Alerts:")
    critical.show(truncate=False)
else:
    print("No critical security alerts. System is secure.")
