# Databricks notebook source

# MAGIC %md
# MAGIC # 03 - Quality Evaluation (Daily)
# MAGIC
# MAGIC MLflow-as-judge quality scores and drift detection.
# MAGIC Uses AgentBricks FinOps Assistant to assess quality trends.

# COMMAND ----------

dbutils.widgets.text("catalog", "finops_monitor", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Quality Data

# COMMAND ----------

from datetime import datetime, timedelta
import json

now = datetime.utcnow()

# Recent quality scores
df_recent = spark.sql(f"""
    SELECT endpoint_name,
           AVG(relevance_score) as avg_relevance,
           AVG(faithfulness_score) as avg_faithfulness,
           AVG(safety_score) as avg_safety,
           AVG(coherence_score) as avg_coherence,
           AVG(overall_score) as avg_overall,
           SUM(eval_count) as total_evals,
           MAX(CASE WHEN drift_detected THEN 1 ELSE 0 END) as drift_detected
    FROM {CATALOG}.{SCHEMA}.quality_scores
    WHERE eval_date >= current_timestamp() - INTERVAL 7 DAYS
    GROUP BY endpoint_name
""")

# Historical baseline (previous 7 days)
df_baseline = spark.sql(f"""
    SELECT endpoint_name,
           AVG(overall_score) as baseline_overall,
           AVG(relevance_score) as baseline_relevance,
           AVG(faithfulness_score) as baseline_faithfulness
    FROM {CATALOG}.{SCHEMA}.quality_scores
    WHERE eval_date >= current_timestamp() - INTERVAL 14 DAYS
      AND eval_date < current_timestamp() - INTERVAL 7 DAYS
    GROUP BY endpoint_name
""")

# Drift trend (daily)
df_trend = spark.sql(f"""
    SELECT DATE(eval_date) as date,
           endpoint_name,
           overall_score
    FROM {CATALOG}.{SCHEMA}.quality_scores
    WHERE eval_date >= current_timestamp() - INTERVAL 30 DAYS
    ORDER BY date
""")

recent_data = [row.asDict() for row in df_recent.collect()]
baseline_data = [row.asDict() for row in df_baseline.collect()]
trend_data = [row.asDict() for row in df_trend.collect()]

print(f"Quality data: {len(recent_data)} endpoints, {len(trend_data)} daily records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call AgentBricks for Quality Analysis

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
        "You are the AI FinOps Quality Evaluator. Analyze MLflow quality scores, "
        "detect drift patterns, and recommend retraining. "
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


quality_prompt = f"""Analyze quality evaluation scores for our GenAI agent endpoints.

## Quality Thresholds:
- Overall score: >= 0.80 (acceptable), >= 0.90 (good)
- Drift alert: >10% decline over 7 days
- Safety: must be >= 0.90 at all times

## Current Week Scores:
{serialize_for_prompt(recent_data)}

## Previous Week Baseline:
{serialize_for_prompt(baseline_data)}

## 30-Day Trend (sample):
{serialize_for_prompt(trend_data[:20])}

Return JSON with:
- summary: Overall quality status
- drift_alerts: Endpoints with quality drift (endpoint_name, metric, current, baseline, change_pct)
- quality_report: Per-endpoint quality assessment (endpoint_name, status, scores, notes)
- recommendations: Retraining and improvement actions
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
        metric = next((m for m in recent_data if m.get("endpoint_name") == ep_name), {})
        drift = any(d.get("endpoint_name") == ep_name for d in analysis.get("drift_alerts", []))

        results.append(Row(
            run_timestamp=run_timestamp,
            endpoint_name=ep_name,
            overall_score=float(metric.get("avg_overall", 0)),
            drift_detected=drift,
            summary=report.get("notes", report.get("status", "")),
            recommendations=json.dumps(analysis.get("recommendations", [])),
            raw_response=response_text,
        ))

except Exception as e:
    print(f"Parse error: {e}")
    for metric in recent_data:
        results.append(Row(
            run_timestamp=run_timestamp,
            endpoint_name=metric.get("endpoint_name", "unknown"),
            overall_score=float(metric.get("avg_overall", 0)),
            drift_detected=bool(metric.get("drift_detected", False)),
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
