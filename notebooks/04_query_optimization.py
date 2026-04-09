# Databricks notebook source

# MAGIC %md
# MAGIC # 04 - Query Optimization (Daily)
# MAGIC
# MAGIC Identifies expensive queries from agent workloads and suggests optimizations.

# COMMAND ----------

dbutils.widgets.text("catalog", "finops_monitor", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Query Data

# COMMAND ----------

from datetime import datetime, timedelta
import json

now = datetime.utcnow()

# Top expensive queries (last 24 hours)
df_expensive = spark.sql(f"""
    SELECT query_id, query_text, user_name, duration_ms,
           bytes_scanned, rows_returned, status, estimated_cost
    FROM {CATALOG}.{SCHEMA}.query_logs
    WHERE timestamp >= current_timestamp() - INTERVAL 24 HOURS
      AND status = 'COMPLETED'
    ORDER BY estimated_cost DESC
    LIMIT 20
""")

# Query patterns (aggregated)
df_patterns = spark.sql(f"""
    SELECT SUBSTRING(query_text, 1, 50) as query_pattern,
           COUNT(*) as execution_count,
           AVG(duration_ms) as avg_duration,
           SUM(bytes_scanned) as total_bytes,
           SUM(estimated_cost) as total_cost
    FROM {CATALOG}.{SCHEMA}.query_logs
    WHERE timestamp >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY SUBSTRING(query_text, 1, 50)
    ORDER BY total_cost DESC
    LIMIT 10
""")

# Failed queries
df_failed = spark.sql(f"""
    SELECT query_text, user_name, COUNT(*) as failure_count
    FROM {CATALOG}.{SCHEMA}.query_logs
    WHERE timestamp >= current_timestamp() - INTERVAL 24 HOURS
      AND status = 'FAILED'
    GROUP BY query_text, user_name
    ORDER BY failure_count DESC
    LIMIT 10
""")

expensive_data = [row.asDict() for row in df_expensive.collect()]
pattern_data = [row.asDict() for row in df_patterns.collect()]
failed_data = [row.asDict() for row in df_failed.collect()]

print(f"Collected: {len(expensive_data)} expensive queries, {len(pattern_data)} patterns, {len(failed_data)} failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call AgentBricks for Query Optimization

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
        "You are the AI FinOps Query Optimizer. Analyze SQL queries for performance issues "
        "and suggest optimizations like partition pruning, predicate pushdown, caching, "
        "and query rewrites. Return JSON with keys: summary, optimizations, total_savings_estimate."
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


query_prompt = f"""Analyze these SQL queries from our GenAI agent workloads and suggest optimizations.

## Top 20 Most Expensive Queries (Last 24h):
{serialize_for_prompt(expensive_data)}

## Query Patterns (Aggregated):
{serialize_for_prompt(pattern_data)}

## Failed Queries:
{serialize_for_prompt(failed_data)}

For each expensive query, provide:
1. What makes it expensive (full scan, missing predicates, inefficient joins, etc.)
2. Specific optimization suggestion (rewrite, add partitioning, use caching, etc.)
3. Estimated cost savings
4. Priority (HIGH/MEDIUM/LOW)

Return JSON with:
- summary: Overall query health
- optimizations: List of (query_id, query_text, issue, suggestion, estimated_savings, priority)
- total_savings_estimate: Total potential savings
"""

response_text, endpoint_used = call_finops_agent(query_prompt)
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

    for opt in analysis.get("optimizations", []):
        results.append(Row(
            run_timestamp=run_timestamp,
            query_id=str(opt.get("query_id", "")),
            query_text=opt.get("query_text", "")[:500],
            estimated_cost=float(opt.get("estimated_savings", 0)),
            duration_ms=0.0,
            optimization_suggestion=opt.get("suggestion", ""),
            priority=opt.get("priority", "MEDIUM"),
            raw_response=response_text,
        ))

except Exception as e:
    print(f"Parse error: {e}")
    for q in expensive_data[:5]:
        results.append(Row(
            run_timestamp=run_timestamp,
            query_id=str(q.get("query_id", "")),
            query_text=str(q.get("query_text", ""))[:500],
            estimated_cost=float(q.get("estimated_cost", 0)),
            duration_ms=float(q.get("duration_ms", 0)),
            optimization_suggestion=response_text[:500] if response_text else "",
            priority="HIGH",
            raw_response=response_text or "",
        ))

if results:
    df_results = spark.createDataFrame(results)
    df_results.write.mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.query_optimization_results")
    print(f"Stored {len(results)} query optimization results")

# COMMAND ----------

print("Query optimization analysis complete.")
