# Databricks notebook source

# MAGIC %md
# MAGIC # 01 - Cost Analysis (Hourly)
# MAGIC
# MAGIC Per-agent, per-model cost attribution and trend analysis.
# MAGIC Uses the AgentBricks FinOps Assistant to analyze billing data and provide insights.

# COMMAND ----------

dbutils.widgets.text("catalog", "att_log_anomaly_catalog", "Catalog")
dbutils.widgets.text("schema", "finops_monitor", "Schema")
dbutils.widgets.text("endpoint_name", "t2t-3ce36a81-endpoint", "Serving Endpoint")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Cost Data

# COMMAND ----------

from datetime import datetime, timedelta
import json

now = datetime.utcnow()
one_week_ago = now - timedelta(days=7)
two_weeks_ago = now - timedelta(days=14)

# Current week costs by agent
df_current = spark.sql(f"""
    SELECT agent_name,
           SUM(total_cost) as total_cost,
           SUM(dbu_usage) as total_dbus,
           COUNT(*) as record_count
    FROM {CATALOG}.{SCHEMA}.billing_usage
    WHERE usage_date >= '{one_week_ago.strftime("%Y-%m-%d")}'
    GROUP BY agent_name
    ORDER BY total_cost DESC
""")

# Previous week for comparison
df_previous = spark.sql(f"""
    SELECT agent_name,
           SUM(total_cost) as total_cost,
           SUM(dbu_usage) as total_dbus
    FROM {CATALOG}.{SCHEMA}.billing_usage
    WHERE usage_date >= '{two_weeks_ago.strftime("%Y-%m-%d")}'
      AND usage_date < '{one_week_ago.strftime("%Y-%m-%d")}'
    GROUP BY agent_name
""")

# Cost by SKU
df_sku = spark.sql(f"""
    SELECT sku_name,
           SUM(total_cost) as total_cost,
           SUM(dbu_usage) as total_dbus
    FROM {CATALOG}.{SCHEMA}.billing_usage
    WHERE usage_date >= '{one_week_ago.strftime("%Y-%m-%d")}'
    GROUP BY sku_name
    ORDER BY total_cost DESC
""")

# Daily trend
df_daily = spark.sql(f"""
    SELECT DATE(usage_date) as date,
           SUM(total_cost) as daily_cost
    FROM {CATALOG}.{SCHEMA}.billing_usage
    WHERE usage_date >= '{one_week_ago.strftime("%Y-%m-%d")}'
    GROUP BY DATE(usage_date)
    ORDER BY date
""")

current_data = [row.asDict() for row in df_current.collect()]
previous_data = [row.asDict() for row in df_previous.collect()]
sku_data = [row.asDict() for row in df_sku.collect()]
daily_data = [row.asDict() for row in df_daily.collect()]

print(f"Collected cost data: {len(current_data)} agents, {len(sku_data)} SKUs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call AgentBricks FinOps Assistant

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()

# Try AgentBricks endpoint first, fall back to foundation model
ENDPOINTS_TO_TRY = [ENDPOINT_NAME]

def call_finops_agent(prompt, system_context=None):
    """Call the FinOps agent with fallback to foundation model."""
    system_msg = system_context or (
        "You are the AI Agentic FinOps Assistant analyzing Databricks platform costs. "
        "Provide structured analysis with specific numbers, trends, and actionable recommendations. "
        "Format output as JSON with keys: summary, findings, recommendations, alerts."
    )

    messages = [
        ChatMessage(role=ChatMessageRole.SYSTEM, content=system_msg),
        ChatMessage(role=ChatMessageRole.USER, content=prompt),
    ]

    for endpoint in ENDPOINTS_TO_TRY:
        try:
            response = w.serving_endpoints.query(
                name=endpoint,
                messages=messages,
                max_tokens=4000,
                temperature=0.3,
            )
            content = response.choices[0].message.content
            return content, endpoint
        except Exception as e:
            print(f"  Endpoint {endpoint} failed: {e}")
            continue

    return None, None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Cost Analysis

# COMMAND ----------

# Prepare context for the agent
def serialize_for_prompt(data):
    """Convert data to string, handling datetime objects."""
    def default_handler(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return str(obj)
    return json.dumps(data, indent=2, default=default_handler)

cost_prompt = f"""Analyze the following Databricks platform cost data and provide a comprehensive cost report.

## Current Week Costs by Agent:
{serialize_for_prompt(current_data)}

## Previous Week Costs by Agent:
{serialize_for_prompt(previous_data)}

## Costs by SKU:
{serialize_for_prompt(sku_data)}

## Daily Cost Trend:
{serialize_for_prompt(daily_data)}

Provide your analysis as JSON with these keys:
- summary: A 2-3 sentence executive summary
- total_cost: Total cost this week
- cost_change_pct: Week-over-week change percentage
- top_spender: The agent with highest cost
- findings: List of key findings (each with agent_name, finding, severity)
- recommendations: List of optimization recommendations (each with action, estimated_savings, priority)
- alerts: List of cost alerts if any anomalies detected
"""

response_text, endpoint_used = call_finops_agent(cost_prompt)
print(f"Response from: {endpoint_used}")
print(f"Response length: {len(response_text) if response_text else 0} chars")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse and Store Results

# COMMAND ----------

from pyspark.sql import Row

run_timestamp = datetime.utcnow()

# Try to parse JSON from the response
try:
    # Extract JSON from response (may be wrapped in markdown code blocks)
    json_text = response_text
    if "```json" in json_text:
        json_text = json_text.split("```json")[1].split("```")[0]
    elif "```" in json_text:
        json_text = json_text.split("```")[1].split("```")[0]

    analysis = json.loads(json_text)

    summary = analysis.get("summary", "")
    total_cost = float(analysis.get("total_cost", 0))
    cost_change = float(analysis.get("cost_change_pct", 0))
    recommendations = json.dumps(analysis.get("recommendations", []))

    results = []
    # Overall analysis
    results.append(Row(
        run_timestamp=run_timestamp,
        analysis_type="weekly_summary",
        agent_name="ALL",
        summary=summary,
        total_cost=total_cost,
        cost_change_pct=cost_change,
        recommendations=recommendations,
        raw_response=response_text,
    ))

    # Per-agent findings
    for finding in analysis.get("findings", []):
        results.append(Row(
            run_timestamp=run_timestamp,
            analysis_type="agent_finding",
            agent_name=finding.get("agent_name", "unknown"),
            summary=finding.get("finding", ""),
            total_cost=0.0,
            cost_change_pct=0.0,
            recommendations="",
            raw_response="",
        ))

except (json.JSONDecodeError, Exception) as e:
    print(f"Could not parse JSON response: {e}")
    # Store raw response
    results = [Row(
        run_timestamp=run_timestamp,
        analysis_type="raw_analysis",
        agent_name="ALL",
        summary=response_text[:500] if response_text else "No response",
        total_cost=sum(r.get("total_cost", 0) for r in current_data),
        cost_change_pct=0.0,
        recommendations="",
        raw_response=response_text or "",
    )]

df_results = spark.createDataFrame(results)
df_results.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.cost_analysis_results")
print(f"Stored {len(results)} cost analysis results")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

latest = spark.sql(f"""
    SELECT * FROM {CATALOG}.{SCHEMA}.cost_analysis_results
    WHERE analysis_type = 'weekly_summary'
    ORDER BY run_timestamp DESC
    LIMIT 1
""").collect()

if latest:
    row = latest[0]
    print(f"Latest Cost Analysis ({row.run_timestamp}):")
    print(f"  Total Cost: ${row.total_cost:,.2f}")
    print(f"  WoW Change: {row.cost_change_pct:+.1f}%")
    print(f"  Summary: {row.summary[:200]}")
