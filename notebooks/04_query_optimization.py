# Databricks notebook source

# MAGIC %md
# MAGIC # 04 - Query Optimization (Daily)
# MAGIC
# MAGIC Identifies expensive queries from `system.query.history` and suggests optimizations.

# COMMAND ----------

dbutils.widgets.text("catalog", "att_log_anomaly_catalog", "Catalog")
dbutils.widgets.text("schema", "finops_monitor", "Schema")
dbutils.widgets.text("endpoint_name", "t2t-3ce36a81-endpoint", "Serving Endpoint")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
ENDPOINT_NAME = dbutils.widgets.get("endpoint_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Query Data from system.query.history

# COMMAND ----------

from datetime import datetime, timedelta
import json

now = datetime.utcnow()

# Top expensive queries (last 24 hours) by duration
df_expensive = spark.sql("""
    SELECT statement_id,
           statement_text,
           executed_by,
           total_duration_ms,
           execution_duration_ms,
           read_bytes,
           read_rows,
           produced_rows,
           read_files,
           execution_status,
           compute.type as compute_type,
           compute.warehouse_id,
           from_result_cache,
           spilled_local_bytes,
           start_time
    FROM system.query.history
    WHERE start_time >= current_timestamp() - INTERVAL 24 HOURS
      AND execution_status = 'FINISHED'
      AND total_duration_ms > 0
    ORDER BY total_duration_ms DESC
    LIMIT 20
""")

# Query patterns aggregated
df_patterns = spark.sql("""
    SELECT SUBSTRING(statement_text, 1, 80) as query_pattern,
           statement_type,
           COUNT(*) as execution_count,
           AVG(total_duration_ms) as avg_duration_ms,
           SUM(read_bytes) as total_bytes_read,
           AVG(read_bytes) as avg_bytes_read,
           SUM(spilled_local_bytes) as total_spilled_bytes
    FROM system.query.history
    WHERE start_time >= current_timestamp() - INTERVAL 24 HOURS
      AND execution_status = 'FINISHED'
    GROUP BY SUBSTRING(statement_text, 1, 80), statement_type
    ORDER BY total_bytes_read DESC
    LIMIT 15
""")

# Failed queries
df_failed = spark.sql("""
    SELECT SUBSTRING(statement_text, 1, 100) as query_text,
           executed_by,
           error_message,
           COUNT(*) as failure_count
    FROM system.query.history
    WHERE start_time >= current_timestamp() - INTERVAL 24 HOURS
      AND execution_status = 'FAILED'
    GROUP BY SUBSTRING(statement_text, 1, 100), executed_by, error_message
    ORDER BY failure_count DESC
    LIMIT 10
""")

# Top users by query volume and cost
df_users = spark.sql("""
    SELECT executed_by,
           COUNT(*) as query_count,
           SUM(total_duration_ms) as total_duration_ms,
           SUM(read_bytes) as total_bytes_read,
           AVG(total_duration_ms) as avg_duration_ms
    FROM system.query.history
    WHERE start_time >= current_timestamp() - INTERVAL 24 HOURS
    GROUP BY executed_by
    ORDER BY total_bytes_read DESC
    LIMIT 10
""")

expensive_data = [row.asDict() for row in df_expensive.collect()]
pattern_data = [row.asDict() for row in df_patterns.collect()]
failed_data = [row.asDict() for row in df_failed.collect()]
user_data = [row.asDict() for row in df_users.collect()]

print(f"Collected: {len(expensive_data)} expensive queries, {len(pattern_data)} patterns, {len(failed_data)} failed, {len(user_data)} users")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Call AgentBricks for Query Optimization

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()

ENDPOINTS_TO_TRY = [ENDPOINT_NAME]

def call_finops_agent(prompt):
    system_msg = (
        "You are the AI FinOps Query Optimizer. Analyze SQL queries from system.query.history "
        "for performance issues and suggest optimizations like partition pruning, predicate pushdown, "
        "caching, and query rewrites. Return JSON with keys: summary, optimizations, total_savings_estimate."
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


query_prompt = f"""Analyze these SQL queries from system.query.history and suggest optimizations.

## Top 20 Most Expensive Queries (Last 24h) by Duration:
{serialize_for_prompt(expensive_data)}

## Query Patterns (Aggregated by first 80 chars):
{serialize_for_prompt(pattern_data)}

## Failed Queries:
{serialize_for_prompt(failed_data)}

## Top Users by Data Scanned:
{serialize_for_prompt(user_data)}

For each expensive query, provide:
1. What makes it expensive (full scan, missing predicates, spills, inefficient joins, etc.)
2. Specific optimization suggestion
3. Estimated improvement
4. Priority (HIGH/MEDIUM/LOW)

Return JSON with:
- summary: Overall query health assessment
- optimizations: List of (query_id, query_text, issue, suggestion, estimated_savings, priority)
- total_savings_estimate: Total potential time savings description
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
            query_id=str(opt.get("query_id", opt.get("statement_id", ""))),
            query_text=str(opt.get("query_text", ""))[:500],
            estimated_cost=float(opt.get("estimated_savings", 0)) if isinstance(opt.get("estimated_savings"), (int, float)) else 0.0,
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
            query_id=str(q.get("statement_id", "")),
            query_text=str(q.get("statement_text", ""))[:500],
            estimated_cost=0.0,
            duration_ms=float(q.get("total_duration_ms", 0)),
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
