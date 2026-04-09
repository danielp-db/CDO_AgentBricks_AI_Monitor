import os
import json
import uuid
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from databricks import sql as dbsql

app = FastAPI(title="FinOps AI Assistant")

CATALOG = os.environ.get("DATABRICKS_CATALOG", "finops_monitor")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "default")

FINOPS_ENDPOINTS = [
    "finops-assistant-agent",
    "databricks-meta-llama-3-3-70b-instruct",
]

# Mount static files
static_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")


def get_sql_connection():
    """Get Databricks SQL connection using app service principal."""
    w = WorkspaceClient()
    # Find a warehouse to use
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouse available")
    wh = warehouses[0]

    return dbsql.connect(
        server_hostname=w.config.host.replace("https://", ""),
        http_path=f"/sql/1.0/warehouses/{wh.id}",
        credentials_provider=lambda: w.config._header_factory(),
    )


def run_query(query: str) -> list[dict]:
    """Execute a SQL query and return results as list of dicts."""
    conn = get_sql_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


def serialize_result(data):
    """JSON-serialize results handling datetime objects."""
    def default_handler(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if hasattr(obj, "isoformat"):
            return obj.isoformat()
        return str(obj)
    return json.loads(json.dumps(data, default=default_handler))


# --------------- API Routes ---------------

@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the main dashboard page."""
    with open(os.path.join(static_dir, "index.html")) as f:
        return HTMLResponse(content=f.read())


@app.get("/api/health")
async def health():
    return {"status": "ok", "catalog": CATALOG, "schema": SCHEMA}


@app.get("/api/overview")
async def overview():
    """Dashboard overview: latest metrics across all capabilities."""
    try:
        cost = run_query(f"""
            SELECT summary, total_cost, cost_change_pct, run_timestamp
            FROM {CATALOG}.{SCHEMA}.cost_analysis_results
            WHERE analysis_type = 'weekly_summary'
            ORDER BY run_timestamp DESC LIMIT 1
        """)

        perf = run_query(f"""
            SELECT endpoint_name, sla_status, avg_latency_ms, error_rate
            FROM {CATALOG}.{SCHEMA}.performance_results
            WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM {CATALOG}.{SCHEMA}.performance_results)
        """)

        quality = run_query(f"""
            SELECT endpoint_name, overall_score, drift_detected
            FROM {CATALOG}.{SCHEMA}.quality_results
            WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM {CATALOG}.{SCHEMA}.quality_results)
        """)

        security = run_query(f"""
            SELECT severity, COUNT(*) as count
            FROM {CATALOG}.{SCHEMA}.security_results
            WHERE run_timestamp >= current_timestamp() - INTERVAL 1 HOUR
            GROUP BY severity
        """)

        anomalies = run_query(f"""
            SELECT anomaly_type, severity, COUNT(*) as count
            FROM {CATALOG}.{SCHEMA}.anomaly_results
            WHERE run_timestamp >= current_timestamp() - INTERVAL 1 HOUR
            GROUP BY anomaly_type, severity
        """)

        return JSONResponse(content=serialize_result({
            "cost": cost,
            "performance": perf,
            "quality": quality,
            "security": security,
            "anomalies": anomalies,
        }))
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/costs")
async def costs():
    """Cost analysis data."""
    try:
        # Latest analysis
        latest = run_query(f"""
            SELECT * FROM {CATALOG}.{SCHEMA}.cost_analysis_results
            ORDER BY run_timestamp DESC LIMIT 20
        """)

        # Daily trend
        trend = run_query(f"""
            SELECT DATE(usage_date) as date,
                   agent_name,
                   SUM(total_cost) as daily_cost
            FROM {CATALOG}.{SCHEMA}.billing_usage
            WHERE usage_date >= current_timestamp() - INTERVAL 14 DAYS
            GROUP BY DATE(usage_date), agent_name
            ORDER BY date
        """)

        # By SKU
        by_sku = run_query(f"""
            SELECT sku_name, SUM(total_cost) as total
            FROM {CATALOG}.{SCHEMA}.billing_usage
            WHERE usage_date >= current_timestamp() - INTERVAL 7 DAYS
            GROUP BY sku_name ORDER BY total DESC
        """)

        return JSONResponse(content=serialize_result({
            "latest": latest,
            "trend": trend,
            "by_sku": by_sku,
        }))
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/performance")
async def performance():
    """Performance monitoring data."""
    try:
        latest = run_query(f"""
            SELECT * FROM {CATALOG}.{SCHEMA}.performance_results
            WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM {CATALOG}.{SCHEMA}.performance_results)
        """)

        latency_trend = run_query(f"""
            SELECT DATE_TRUNC('HOUR', timestamp) as hour,
                   endpoint_name,
                   AVG(avg_latency_ms) as avg_latency,
                   AVG(p95_latency_ms) as p95_latency
            FROM {CATALOG}.{SCHEMA}.serving_metrics
            WHERE timestamp >= current_timestamp() - INTERVAL 24 HOURS
            GROUP BY 1, 2 ORDER BY 1
        """)

        return JSONResponse(content=serialize_result({
            "latest": latest,
            "latency_trend": latency_trend,
        }))
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/quality")
async def quality():
    """Quality evaluation data."""
    try:
        latest = run_query(f"""
            SELECT * FROM {CATALOG}.{SCHEMA}.quality_results
            WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM {CATALOG}.{SCHEMA}.quality_results)
        """)

        trend = run_query(f"""
            SELECT DATE(eval_date) as date, endpoint_name, overall_score
            FROM {CATALOG}.{SCHEMA}.quality_scores
            WHERE eval_date >= current_timestamp() - INTERVAL 30 DAYS
            ORDER BY date
        """)

        return JSONResponse(content=serialize_result({
            "latest": latest,
            "trend": trend,
        }))
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/security")
async def security():
    """Security audit data."""
    try:
        alerts = run_query(f"""
            SELECT * FROM {CATALOG}.{SCHEMA}.security_results
            ORDER BY run_timestamp DESC LIMIT 50
        """)

        by_severity = run_query(f"""
            SELECT severity, COUNT(*) as count
            FROM {CATALOG}.{SCHEMA}.security_results
            WHERE run_timestamp >= current_timestamp() - INTERVAL 24 HOURS
            GROUP BY severity
        """)

        return JSONResponse(content=serialize_result({
            "alerts": alerts,
            "by_severity": by_severity,
        }))
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/anomalies")
async def anomalies():
    """Anomaly detection data."""
    try:
        recent = run_query(f"""
            SELECT * FROM {CATALOG}.{SCHEMA}.anomaly_results
            ORDER BY run_timestamp DESC LIMIT 50
        """)

        by_type = run_query(f"""
            SELECT anomaly_type, severity, COUNT(*) as count
            FROM {CATALOG}.{SCHEMA}.anomaly_results
            WHERE run_timestamp >= current_timestamp() - INTERVAL 24 HOURS
            GROUP BY anomaly_type, severity
        """)

        return JSONResponse(content=serialize_result({
            "recent": recent,
            "by_type": by_type,
        }))
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/queries")
async def queries():
    """Query optimization data."""
    try:
        optimizations = run_query(f"""
            SELECT * FROM {CATALOG}.{SCHEMA}.query_optimization_results
            ORDER BY run_timestamp DESC LIMIT 20
        """)

        return JSONResponse(content=serialize_result({
            "optimizations": optimizations,
        }))
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)


# --------------- Chat (Natural Language Interface) ---------------

@app.post("/api/chat")
async def chat(request: Request):
    """Natural language interface to the FinOps assistant."""
    body = await request.json()
    user_message = body.get("message", "")
    session_id = body.get("session_id", str(uuid.uuid4()))

    if not user_message:
        return JSONResponse(content={"error": "No message provided"}, status_code=400)

    # Gather context from recent data
    try:
        context_cost = run_query(f"""
            SELECT summary FROM {CATALOG}.{SCHEMA}.cost_analysis_results
            WHERE analysis_type = 'weekly_summary'
            ORDER BY run_timestamp DESC LIMIT 1
        """)
        context_perf = run_query(f"""
            SELECT endpoint_name, sla_status, avg_latency_ms
            FROM {CATALOG}.{SCHEMA}.performance_results
            WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM {CATALOG}.{SCHEMA}.performance_results)
        """)
        context_security = run_query(f"""
            SELECT severity, alert_type, description
            FROM {CATALOG}.{SCHEMA}.security_results
            WHERE run_timestamp >= current_timestamp() - INTERVAL 1 HOUR
            ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 ELSE 3 END
            LIMIT 5
        """)
        context_anomalies = run_query(f"""
            SELECT anomaly_type, resource_name, severity, description
            FROM {CATALOG}.{SCHEMA}.anomaly_results
            WHERE run_timestamp >= current_timestamp() - INTERVAL 1 HOUR
            LIMIT 5
        """)
    except Exception:
        context_cost = []
        context_perf = []
        context_security = []
        context_anomalies = []

    system_prompt = f"""You are the AI Agentic FinOps Assistant for Databricks GenAI workloads.
You have access to the following real-time monitoring data:

Cost Summary: {json.dumps(context_cost, default=str)}
Performance Status: {json.dumps(context_perf, default=str)}
Recent Security Alerts: {json.dumps(context_security, default=str)}
Recent Anomalies: {json.dumps(context_anomalies, default=str)}

Answer questions about agent costs, performance, quality, security, and anomalies.
Be specific with numbers, timestamps, and agent names. Provide actionable recommendations."""

    # Call the FinOps agent
    w = WorkspaceClient()
    response_text = None
    endpoint_used = None

    messages = [
        ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt),
        ChatMessage(role=ChatMessageRole.USER, content=user_message),
    ]

    for endpoint in FINOPS_ENDPOINTS:
        try:
            response = w.serving_endpoints.query(
                name=endpoint,
                messages=messages,
                max_tokens=4000,
                temperature=0.5,
            )
            response_text = response.choices[0].message.content
            endpoint_used = endpoint
            break
        except Exception:
            continue

    if not response_text:
        response_text = "I'm sorry, I couldn't process your request. The FinOps agent endpoint is currently unavailable."

    # Store chat history
    try:
        conn = get_sql_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""INSERT INTO {CATALOG}.{SCHEMA}.chat_history
                (timestamp, session_id, user_message, assistant_response, model_used)
                VALUES (current_timestamp(), ?, ?, ?, ?)""",
            [session_id, user_message, response_text, endpoint_used or "none"],
        )
        conn.close()
    except Exception:
        pass

    return JSONResponse(content={
        "response": response_text,
        "session_id": session_id,
        "model_used": endpoint_used,
    })
