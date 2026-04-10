# Databricks notebook source

# MAGIC %md
# MAGIC # 01 - Cost Analysis (Hourly)
# MAGIC
# MAGIC Per-SKU, per-endpoint cost attribution and trend analysis using `system.billing.usage`.
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
# MAGIC ## Collect Cost Data from system.billing.usage

# COMMAND ----------

from datetime import datetime, timedelta
import json

now = datetime.utcnow()

# Current week costs by SKU
df_current = spark.sql("""
    SELECT sku_name,
           billing_origin_product,
           usage_metadata.endpoint_name as endpoint_name,
           SUM(usage_quantity) as total_dbus,
           COUNT(*) as record_count
    FROM system.billing.usage
    WHERE usage_date >= current_date() - INTERVAL 7 DAYS
    GROUP BY sku_name, billing_origin_product, usage_metadata.endpoint_name
    ORDER BY total_dbus DESC
    LIMIT 50
""")

# Previous week for comparison
df_previous = spark.sql("""
    SELECT sku_name,
           SUM(usage_quantity) as total_dbus
    FROM system.billing.usage
    WHERE usage_date >= current_date() - INTERVAL 14 DAYS
      AND usage_date < current_date() - INTERVAL 7 DAYS
    GROUP BY sku_name
""")

# Cost by billing origin product
df_by_product = spark.sql("""
    SELECT billing_origin_product,
           SUM(usage_quantity) as total_dbus,
           COUNT(DISTINCT sku_name) as sku_count
    FROM system.billing.usage
    WHERE usage_date >= current_date() - INTERVAL 7 DAYS
    GROUP BY billing_origin_product
    ORDER BY total_dbus DESC
""")

# Daily trend
df_daily = spark.sql("""
    SELECT usage_date as date,
           SUM(usage_quantity) as daily_dbus
    FROM system.billing.usage
    WHERE usage_date >= current_date() - INTERVAL 14 DAYS
    GROUP BY usage_date
    ORDER BY usage_date
""")

# Serving / agent-specific costs
df_serving = spark.sql("""
    SELECT usage_metadata.endpoint_name as endpoint_name,
           sku_name,
           SUM(usage_quantity) as total_dbus
    FROM system.billing.usage
    WHERE usage_date >= current_date() - INTERVAL 7 DAYS
      AND usage_metadata.endpoint_name IS NOT NULL
    GROUP BY usage_metadata.endpoint_name, sku_name
    ORDER BY total_dbus DESC
    LIMIT 30
""")

current_data = [row.asDict() for row in df_current.collect()]
previous_data = [row.asDict() for row in df_previous.collect()]
product_data = [row.asDict() for row in df_by_product.collect()]
daily_data = [row.asDict() for row in df_daily.collect()]
serving_data = [row.asDict() for row in df_serving.collect()]

print(f"Collected cost data: {len(current_data)} SKU/endpoint combos, {len(product_data)} products, {len(serving_data)} serving endpoints")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call AgentBricks FinOps Assistant

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()

ENDPOINTS_TO_TRY = [ENDPOINT_NAME]

def call_finops_agent(prompt, system_context=None):
    """Call the FinOps agent."""
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

def serialize_for_prompt(data):
    """Convert data to string, handling datetime objects."""
    def default_handler(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        return str(obj)
    return json.dumps(data, indent=2, default=default_handler)

cost_prompt = f"""Analyze the following Databricks platform cost data from system.billing.usage and provide a comprehensive cost report.

## Current Week Usage by SKU and Endpoint (DBUs):
{serialize_for_prompt(current_data)}

## Previous Week Usage by SKU (DBUs):
{serialize_for_prompt(previous_data)}

## Usage by Billing Product:
{serialize_for_prompt(product_data)}

## Serving Endpoint Usage (DBUs):
{serialize_for_prompt(serving_data)}

## Daily DBU Trend (14 days):
{serialize_for_prompt(daily_data)}

Provide your analysis as JSON with these keys:
- summary: A 2-3 sentence executive summary
- total_cost: Total estimated cost this week (use $0.07/DBU as approximate rate)
- cost_change_pct: Week-over-week change percentage in DBU consumption
- top_spender: The SKU or endpoint with highest DBU usage
- findings: List of key findings (each with name, finding, severity)
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

try:
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

    for finding in analysis.get("findings", []):
        results.append(Row(
            run_timestamp=run_timestamp,
            analysis_type="agent_finding",
            agent_name=finding.get("name", finding.get("agent_name", "unknown")),
            summary=finding.get("finding", ""),
            total_cost=0.0,
            cost_change_pct=0.0,
            recommendations="",
            raw_response="",
        ))

except (json.JSONDecodeError, Exception) as e:
    print(f"Could not parse JSON response: {e}")
    results = [Row(
        run_timestamp=run_timestamp,
        analysis_type="raw_analysis",
        agent_name="ALL",
        summary=response_text[:500] if response_text else "No response",
        total_cost=0.0,
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
