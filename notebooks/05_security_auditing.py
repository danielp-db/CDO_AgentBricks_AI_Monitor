# Databricks notebook source

# MAGIC %md
# MAGIC # 05 - Security Auditing (Every 5 min)
# MAGIC
# MAGIC Unauthorized access detection, permission drift monitoring.
# MAGIC Uses `system.access.audit` for real audit event analysis.

# COMMAND ----------

dbutils.widgets.text("catalog", "att_log_anomaly_catalog", "Catalog")
dbutils.widgets.text("schema", "finops_monitor", "Schema")
dbutils.widgets.text("endpoint_name", "t2t-3ce36a81-endpoint", "Serving Endpoint")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Security Data from system.access.audit

# COMMAND ----------

from datetime import datetime, timedelta
import json

now = datetime.utcnow()

# Failed authentication attempts (last 30 min)
df_failed_logins = spark.sql("""
    SELECT user_identity.email as user_email,
           source_ip_address,
           COUNT(*) as attempt_count,
           MIN(event_time) as first_attempt,
           MAX(event_time) as last_attempt
    FROM system.access.audit
    WHERE event_time >= current_timestamp() - INTERVAL 30 MINUTES
      AND action_name IN ('login', 'tokenLogin', 'aadTokenLogin')
      AND response.status_code >= 400
    GROUP BY user_identity.email, source_ip_address
    HAVING COUNT(*) >= 2
    ORDER BY attempt_count DESC
""")

# Permission / grant changes (last hour)
df_permission_changes = spark.sql("""
    SELECT user_identity.email as user_email,
           action_name,
           service_name,
           event_time,
           source_ip_address,
           request_params
    FROM system.access.audit
    WHERE event_time >= current_timestamp() - INTERVAL 1 HOUR
      AND action_name IN (
          'updatePermissions', 'changePermissions',
          'grantPermission', 'revokePermission',
          'updateSharePermissions'
      )
    ORDER BY event_time DESC
""")

# Token creation events
df_tokens = spark.sql("""
    SELECT user_identity.email as user_email,
           COUNT(*) as token_count,
           MIN(event_time) as first_created,
           MAX(event_time) as last_created
    FROM system.access.audit
    WHERE event_time >= current_timestamp() - INTERVAL 1 HOUR
      AND action_name IN ('createToken', 'generateToken', 'create')
      AND service_name = 'tokens'
    GROUP BY user_identity.email
    HAVING COUNT(*) >= 2
""")

# High-volume actions (potential automation or abuse)
df_high_volume = spark.sql("""
    SELECT user_identity.email as user_email,
           action_name,
           service_name,
           COUNT(*) as action_count
    FROM system.access.audit
    WHERE event_time >= current_timestamp() - INTERVAL 30 MINUTES
    GROUP BY user_identity.email, action_name, service_name
    HAVING COUNT(*) >= 50
    ORDER BY action_count DESC
""")

# Recent sensitive actions
df_sensitive = spark.sql("""
    SELECT user_identity.email as user_email,
           action_name,
           service_name,
           source_ip_address,
           event_time,
           response.status_code as status_code
    FROM system.access.audit
    WHERE event_time >= current_timestamp() - INTERVAL 30 MINUTES
      AND (
          action_name IN ('deleteCluster', 'deletePipeline', 'deleteEndpoint',
                          'deleteWarehouse', 'dropTable', 'dropSchema', 'dropCatalog')
          OR (service_name = 'secrets' AND action_name IN ('putSecret', 'deleteSecret'))
      )
    ORDER BY event_time DESC
""")

failed_logins = [row.asDict() for row in df_failed_logins.collect()]
perm_changes = [row.asDict() for row in df_permission_changes.collect()]
token_events = [row.asDict() for row in df_tokens.collect()]
high_volume = [row.asDict() for row in df_high_volume.collect()]
sensitive_actions = [row.asDict() for row in df_sensitive.collect()]

print(f"Security data: {len(failed_logins)} failed login patterns, {len(perm_changes)} permission changes, {len(token_events)} token anomalies, {len(high_volume)} high-volume users, {len(sensitive_actions)} sensitive actions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call AgentBricks for Security Analysis

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()

ENDPOINTS_TO_TRY = [ENDPOINT_NAME]

def call_finops_agent(prompt):
    system_msg = (
        "You are the AI FinOps Security Auditor. Analyze audit logs from system.access.audit "
        "for security threats including brute force attacks, unauthorized access, permission drift, "
        "and suspicious activity. Classify severity: CRITICAL, HIGH, MEDIUM, LOW. "
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


security_prompt = f"""Analyze the following security audit data from system.access.audit and identify threats.

## Failed Authentication Attempts (Last 30 min, 2+ attempts):
{serialize_for_prompt(failed_logins)}

## Permission / Grant Changes (Last Hour):
{serialize_for_prompt(perm_changes)}

## Token Creation Events (2+ in 1 hour):
{serialize_for_prompt(token_events)}

## High-Volume Actions (50+ in 30 min per user):
{serialize_for_prompt(high_volume)}

## Sensitive Destructive Actions (Last 30 min):
{serialize_for_prompt(sensitive_actions)}

Severity classification:
- CRITICAL: Active attack or confirmed breach (10+ failed logins, mass deletions, mass permission changes)
- HIGH: Likely malicious (repeated failed logins, unusual admin actions, secret modifications)
- MEDIUM: Requires investigation (permission changes, token creation spikes, high-volume automation)
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
    if failed_logins or high_volume or sensitive_actions:
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
