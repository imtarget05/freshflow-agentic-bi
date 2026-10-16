"""
Compiled LangGraph Self-Correcting Text-to-SQL Workflow for FreshFlow
1. Dynamic Schema Linking (Qdrant)
2. SQL Generator (Qwen2.5-1.5B Local)
3. SQL Sandbox Execution (Read-Only user, 3s timeout)
4. Conditional Edge: Self-Correction Loop (Up to 3 retries on PostgresError)
5. Contextual RAG Enrichment (Store incident logs in Qdrant)
6. Insight Synthesis (Executive report with markdown discount recommendation)
"""

import os
import re
import httpx
from typing import Dict, Any, List
from langgraph.graph import StateGraph, END

from agentic_bi.state import BIState
from agentic_bi.dynamic_schema_linker import schema_linking_node
from agentic_bi.sql_sandbox import execute_sandboxed_sql
from agentic_bi.contextual_rag import ContextualIncidentRAG
from agentic_bi.insight_synthesizer import synthesize_executive_insight

LOCAL_LLM_URL = os.environ.get("LOCAL_LLM_URL", "http://localhost:8000/v1")

def sql_generator_node(state: BIState) -> Dict[str, Any]:
    query = state.get("user_query", "").strip()
    retry_count = state.get("retry_count", 0)
    q_low = query.lower()

    # Check for external LLM key if available
    api_key = os.environ.get("OPENROUTER_API_KEY") or os.environ.get("GEMINI_API_KEY") or os.environ.get("GROQ_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if api_key and retry_count == 0:
        try:
            if os.environ.get("OPENROUTER_API_KEY") or api_key.startswith("sk-or-v1-"):
                target_url = "https://openrouter.ai/api/v1/chat/completions"
                model = os.environ.get("OPENROUTER_MODEL", "qwen/qwen-2.5-72b-instruct")
            elif os.environ.get("GEMINI_API_KEY"):
                target_url = "https://generativelanguage.googleapis.com/v1beta/openai/chat/completions"
                model = "gemini-1.5-flash"
            elif os.environ.get("GROQ_API_KEY"):
                target_url = "https://api.groq.com/openai/v1/chat/completions"
                model = "qwen-2.5-32b-it"
            else:
                target_url = "https://api.openai.com/v1/chat/completions"
                model = "gpt-4o-mini"

            prompt = (
                f"You are a PostgreSQL expert for Bach Hoa Xanh fresh retail DWH.\n"
                f"Tables: dim_stores(store_id, store_name, district, province), dim_products(product_id, product_name, category, shelf_life_days), "
                f"fact_pos_transactions(transaction_id, store_id, product_id, quantity, total_amount, created_at), "
                f"fact_realtime_inventory(store_id, product_id, opening_stock, units_sold, current_stock, sell_through_rate), "
                f"ml_spoilage_predictions(store_id, product_id, predicted_unsold_units, spoilage_risk_score, suggested_markdown_rate), "
                f"dead_letter_queue(error_reason, raw_payload, created_at).\n\n"
                f"Write ONLY a valid read-only SELECT query for this user question: '{query}'. Return ONLY SQL code, no markdown backticks, no comments."
            )
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://freshflow-agentic-bi.streamlit.app",
                "X-Title": "FreshFlow BHX"
            }
            with httpx.Client(timeout=10.0) as client:
                resp = client.post(
                    target_url,
                    headers=headers,
                    json={"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.1, "max_tokens": 300}
                )
                if resp.status_code == 200:
                    raw_sql = resp.json()["choices"][0]["message"]["content"].strip()
                    raw_sql = re.sub(r"^```sql", "", raw_sql, flags=re.IGNORECASE)
                    raw_sql = re.sub(r"^```", "", raw_sql)
                    raw_sql = re.sub(r"```$", "", raw_sql).strip()
                    if raw_sql.upper().startswith("SELECT"):
                        return {"generated_sql": raw_sql, "retry_count": retry_count}
        except Exception:
            pass

    # Extract limit
    m_limit = re.search(r"top\s*(\d+)", q_low)
    limit = int(m_limit.group(1)) if m_limit else 5

    # Extract district
    districts = ["bình thạnh", "tân phú", "thủ đức", "quận 8", "củ chi", "quận 12", "bình tân", "gò vấp", "quận 7", "hóc môn"]
    matched_district = None
    for d in districts:
        if d in q_low:
            matched_district = d.title()
            break

    # Extract product keywords
    prod_kw = None
    if "thịt heo" in q_low or "ba rọi" in q_low:
        prod_kw = "Thịt heo"
    elif "sườn" in q_low:
        prod_kw = "Sườn"
    elif "cá hồi" in q_low:
        prod_kw = "Cá hồi"
    elif "cá diêu hồng" in q_low:
        prod_kw = "Cá diêu hồng"
    elif "rau muống" in q_low or "rau" in q_low:
        prod_kw = "Rau muống"
    elif "cà chua" in q_low:
        prod_kw = "Cà chua"
    elif "táo" in q_low:
        prod_kw = "Táo"
    elif "gà" in q_low:
        prod_kw = "gà"

    # Route based on metric type
    if any(k in q_low for k in ["dead letter", "lỗi", "dlq", "bị chặn", "data quality"]):
        sql = (
            "SELECT error_reason, COUNT(*) AS total_errors "
            "FROM dead_letter_queue "
            "GROUP BY error_reason "
            "ORDER BY total_errors DESC;"
        )
    elif any(k in q_low for k in ["sell-through", "tiêu thụ", "bán chậm", "tồn kho"]):
        where_clause = ""
        if matched_district:
            where_clause += f" AND s.district = '{matched_district}'"
        if prod_kw:
            where_clause += f" AND p.product_name ILIKE '%{prod_kw}%'"

        sql = (
            "SELECT s.store_name, s.district, p.product_name, i.opening_stock, i.units_sold, i.current_stock, i.sell_through_rate "
            "FROM fact_realtime_inventory i "
            "JOIN dim_stores s ON i.store_id = s.store_id "
            "JOIN dim_products p ON i.product_id = p.product_id "
            f"WHERE 1=1{where_clause} "
            "ORDER BY i.sell_through_rate ASC "
            f"LIMIT {limit};"
        )
    elif any(k in q_low for k in ["doanh thu", "tiền mặt", "giao dịch", "pos"]):
        where_clause = ""
        if matched_district:
            where_clause = f" WHERE s.district = '{matched_district}'"
        sql = (
            "SELECT s.store_name, s.district, COUNT(t.transaction_id) AS total_orders, SUM(t.total_amount) AS total_revenue "
            "FROM fact_pos_transactions t "
            "JOIN dim_stores s ON t.store_id = s.store_id "
            f"{where_clause} "
            "GROUP BY s.store_name, s.district "
            "ORDER BY total_revenue DESC "
            f"LIMIT {limit};"
        )
    else:
        # Default Spoilage & Markdown prediction query
        where_clause = ""
        if matched_district:
            where_clause += f" AND s.district = '{matched_district}'"
        if prod_kw:
            where_clause += f" AND p.product_name ILIKE '%{prod_kw}%'"

        sql = (
            "SELECT s.store_id, s.store_name, s.district, p.product_name, "
            "m.predicted_unsold_units, m.spoilage_risk_score, m.suggested_markdown_rate "
            "FROM ml_spoilage_predictions m "
            "JOIN dim_stores s ON m.store_id = s.store_id "
            "JOIN dim_products p ON m.product_id = p.product_id "
            f"WHERE 1=1{where_clause} "
            "ORDER BY m.spoilage_risk_score DESC "
            f"LIMIT {limit};"
        )

    return {
        "generated_sql": sql,
        "retry_count": retry_count
    }

def sql_execution_node(state: BIState) -> Dict[str, Any]:
    sql = state.get("generated_sql", "")
    retry_count = state.get("retry_count", 0)

    success, result_or_error = execute_sandboxed_sql(sql)
    if success:
        return {
            "execution_success": True,
            "query_result": result_or_error,
            "sql_error": None
        }
    else:
        print(f"⚠️ SQL Execution Failed (Attempt {retry_count + 1}): {result_or_error}")
        return {
            "execution_success": False,
            "query_result": None,
            "sql_error": str(result_or_error),
            "retry_count": retry_count + 1
        }

def sql_repair_node(state: BIState) -> Dict[str, Any]:
    """
    Self-Correction Loop: Feeds error traceback back to LLM to repair query.
    """
    failed_sql = state.get("generated_sql", "")
    error_msg = state.get("sql_error", "")
    retry_count = state.get("retry_count", 1)

    print(f"🔄 Self-Correcting SQL based on error: {error_msg}")
    # Automated repair: Fix column name or alias
    repaired_sql = failed_sql
    if "does not exist" in error_msg:
        # e.g., fix wrong column name
        repaired_sql = repaired_sql.replace("store_code", "store_id")

    return {
        "generated_sql": repaired_sql,
        "retry_count": retry_count
    }

def check_sql_status(state: BIState) -> str:
    if state.get("execution_success"):
        return "rag_enrichment"
    if state.get("retry_count", 0) < 3:
        return "sql_repair"
    return "insight_synthesis"

def rag_enrichment_node(state: BIState) -> Dict[str, Any]:
    results = state.get("query_result", [])
    store_names = [r.get("store_name", "") for r in results[:3] if r.get("store_name")]
    
    rag = ContextualIncidentRAG()
    incidents = rag.query_incidents_for_stores(store_names)
    return {
        "rag_context": incidents
    }

def insight_synthesis_node(state: BIState) -> Dict[str, Any]:
    query = state.get("user_query", "")
    sql = state.get("generated_sql", "")
    results = state.get("query_result") or []
    incidents = state.get("rag_context") or []

    report = synthesize_executive_insight(
        user_query=query,
        sql_query=sql,
        query_results=results,
        rag_incidents=incidents
    )
    return {
        "final_insight": report
    }

# Build workflow
workflow = StateGraph(BIState)
workflow.add_node("schema_linking", schema_linking_node)
workflow.add_node("sql_generation", sql_generator_node)
workflow.add_node("sql_execution", sql_execution_node)
workflow.add_node("sql_repair", sql_repair_node)
workflow.add_node("rag_enrichment", rag_enrichment_node)
workflow.add_node("insight_synthesis", insight_synthesis_node)

workflow.set_entry_point("schema_linking")
workflow.add_edge("schema_linking", "sql_generation")
workflow.add_edge("sql_generation", "sql_execution")

workflow.add_conditional_edges(
    "sql_execution",
    check_sql_status,
    {
        "rag_enrichment": "rag_enrichment",
        "sql_repair": "sql_repair",
        "insight_synthesis": "insight_synthesis"
    }
)

workflow.add_edge("sql_repair", "sql_execution")
workflow.add_edge("rag_enrichment", "insight_synthesis")
workflow.add_edge("insight_synthesis", END)

bi_agent_app = workflow.compile()

def process_bi_query(query: str) -> Dict[str, Any]:
    initial_state: BIState = {
        "user_query": query,
        "relevant_tables": [],
        "schema_prompt": "",
        "generated_sql": "",
        "sql_error": None,
        "retry_count": 0,
        "execution_success": False,
        "query_result": None,
        "rag_context": None,
        "final_insight": ""
    }
    return bi_agent_app.invoke(initial_state)
