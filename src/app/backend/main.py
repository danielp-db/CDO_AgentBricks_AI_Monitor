import os
import json
import uuid
import logging
from datetime import datetime
from typing import Optional, Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

logger = logging.getLogger("finops")

app = FastAPI(title="FinOps AI Assistant")

CATALOG = os.environ.get("DATABRICKS_CATALOG", "att_log_anomaly_catalog")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "finops_monitor")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "0b11e3b9a1c7aff0")

ENDPOINT_NAME = os.environ.get("DATABRICKS_ENDPOINT_NAME", "t2t-3ce36a81-endpoint")
FINOPS_ENDPOINTS = [ENDPOINT_NAME]

# Singleton WorkspaceClient
_ws: Optional[WorkspaceClient] = None


def get_ws() -> WorkspaceClient:
    global _ws
    if _ws is None:
        _ws = WorkspaceClient()
    return _ws


def run_query(query: str) -> list[dict[str, Any]]:
    """Execute SQL via Statement Execution API."""
    ws = get_ws()
    response = ws.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        wait_timeout="30s",
    )

    if response.status and response.status.state and response.status.state.value == "FAILED":
        error_msg = ""
        if response.status.error:
            error_msg = response.status.error.message or "Unknown SQL error"
        raise RuntimeError(f"SQL error: {error_msg}")

    columns = []
    if response.manifest and response.manifest.schema and response.manifest.schema.columns:
        columns = [col.name for col in response.manifest.schema.columns]

    rows = []
    if response.result and response.result.data_array:
        for row_data in response.result.data_array:
            row = {}
            for idx, col_name in enumerate(columns):
                row[col_name] = row_data[idx] if idx < len(row_data) else None
            rows.append(row)

    return rows


# Cache workspace_id -> workspace_url mapping
_workspace_map: Optional[dict[str, str]] = None


def get_workspace_map() -> dict[str, str]:
    """Return {workspace_id: workspace_url} from system.access.workspaces_latest."""
    global _workspace_map
    if _workspace_map is None:
        try:
            rows = run_query("SELECT workspace_id, workspace_url FROM system.access.workspaces_latest")
            _workspace_map = {r["workspace_id"]: r["workspace_url"] for r in rows}
        except Exception:
            _workspace_map = {}
    return _workspace_map


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

@app.get("/api/workspaces")
async def workspaces():
    """Return workspace_id -> workspace_url mapping."""
    try:
        return JSONResponse(content=get_workspace_map())
    except Exception as e:
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/api/health")
async def health():
    return {"status": "ok", "catalog": CATALOG, "schema": SCHEMA, "warehouse": WAREHOUSE_ID}


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

        perf = run_query("""
            SELECT endpoint_name,
                   workspace_id,
                   AVG(latency_ms) as avg_latency_ms,
                   ROUND(SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 4) as error_rate,
                   CASE
                     WHEN AVG(latency_ms) > 2000 OR SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 5 THEN 'CRITICAL'
                     WHEN AVG(latency_ms) > 1000 OR SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 2 THEN 'WARNING'
                     ELSE 'HEALTHY'
                   END as sla_status
            FROM system.ai_gateway.usage
            WHERE event_time >= current_timestamp() - INTERVAL 1 HOUR
            GROUP BY endpoint_name, workspace_id
        """)

        quality = run_query("""
            SELECT endpoint_name,
                   workspace_id,
                   ROUND(1.0 - (SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 1.0 / GREATEST(COUNT(*), 1)), 4) as overall_score,
                   false as drift_detected
            FROM system.ai_gateway.usage
            WHERE event_time >= current_timestamp() - INTERVAL 7 DAYS
            GROUP BY endpoint_name, workspace_id
            HAVING COUNT(*) >= 5
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
        logger.exception("overview error")
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/costs")
async def costs():
    """Cost analysis data from system.billing.usage."""
    try:
        latest = run_query(f"""
            SELECT * FROM {CATALOG}.{SCHEMA}.cost_analysis_results
            ORDER BY run_timestamp DESC LIMIT 20
        """)

        trend = run_query("""
            SELECT CAST(usage_date AS STRING) as date,
                   billing_origin_product as agent_name,
                   SUM(usage_quantity) as daily_cost
            FROM system.billing.usage
            WHERE usage_date >= current_date() - INTERVAL 14 DAYS
            GROUP BY usage_date, billing_origin_product
            ORDER BY usage_date
        """)

        by_sku = run_query("""
            SELECT sku_name, SUM(usage_quantity) as total
            FROM system.billing.usage
            WHERE usage_date >= current_date() - INTERVAL 7 DAYS
            GROUP BY sku_name ORDER BY total DESC
        """)

        return JSONResponse(content=serialize_result({
            "latest": latest,
            "trend": trend,
            "by_sku": by_sku,
        }))
    except Exception as e:
        logger.exception("costs error")
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/performance")
async def performance():
    """Performance monitoring data from system.ai_gateway.usage."""
    try:
        latest = run_query(f"""
            SELECT * FROM {CATALOG}.{SCHEMA}.performance_results
            WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM {CATALOG}.{SCHEMA}.performance_results)
        """)

        latency_trend = run_query("""
            SELECT CAST(DATE_TRUNC('HOUR', event_time) AS STRING) as hour,
                   endpoint_name,
                   workspace_id,
                   AVG(latency_ms) as avg_latency,
                   PERCENTILE(latency_ms, 0.95) as p95_latency
            FROM system.ai_gateway.usage
            WHERE event_time >= current_timestamp() - INTERVAL 24 HOURS
            GROUP BY DATE_TRUNC('HOUR', event_time), endpoint_name, workspace_id
            ORDER BY 1
        """)

        return JSONResponse(content=serialize_result({
            "latest": latest,
            "latency_trend": latency_trend,
        }))
    except Exception as e:
        logger.exception("performance error")
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.get("/api/quality")
async def quality():
    """Quality evaluation data from system.ai_gateway.usage."""
    try:
        latest = run_query(f"""
            SELECT * FROM {CATALOG}.{SCHEMA}.quality_results
            WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM {CATALOG}.{SCHEMA}.quality_results)
        """)

        trend = run_query("""
            SELECT CAST(DATE(event_time) AS STRING) as date,
                   endpoint_name,
                   workspace_id,
                   ROUND(1.0 - (SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) * 1.0 / COUNT(*)), 4) as overall_score
            FROM system.ai_gateway.usage
            WHERE event_time >= current_timestamp() - INTERVAL 30 DAYS
            GROUP BY DATE(event_time), endpoint_name, workspace_id
            HAVING COUNT(*) >= 5
            ORDER BY date
        """)

        return JSONResponse(content=serialize_result({
            "latest": latest,
            "trend": trend,
        }))
    except Exception as e:
        logger.exception("quality error")
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
        logger.exception("security error")
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
        logger.exception("anomalies error")
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
        logger.exception("queries error")
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
    ws = get_ws()
    response_text = None
    endpoint_used = None

    messages = [
        ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt),
        ChatMessage(role=ChatMessageRole.USER, content=user_message),
    ]

    for endpoint in FINOPS_ENDPOINTS:
        try:
            response = ws.serving_endpoints.query(
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

    # Store chat history (best-effort)
    try:
        run_query(f"""
            INSERT INTO {CATALOG}.{SCHEMA}.chat_history
            (timestamp, session_id, user_message, assistant_response, model_used)
            VALUES (current_timestamp(), '{session_id}', '{user_message.replace("'", "''")}', '{response_text[:2000].replace("'", "''")}', '{endpoint_used or "none"}')
        """)
    except Exception:
        pass

    return JSONResponse(content={
        "response": response_text,
        "session_id": session_id,
        "model_used": endpoint_used,
    })


# Mount static files and serve index.html at root
static_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the main dashboard page."""
    with open(os.path.join(static_dir, "index.html")) as f:
        return HTMLResponse(content=f.read())


app.mount("/static", StaticFiles(directory=static_dir), name="static")
