"""
Unit & Security Tests for SQL Sandbox and Self-Correction Text-to-SQL
"""

import pytest
from agentic_bi.sql_sandbox import execute_sandboxed_sql, clean_and_validate_sql
from agentic_bi.graph import process_bi_query

def test_sql_guardrail_blocks_drop_table():
    malicious = "DROP TABLE fact_realtime_inventory;"
    is_safe, msg = clean_and_validate_sql(malicious)
    assert is_safe is False
    assert "BẢO MẬT" in msg

def test_sql_guardrail_blocks_delete():
    malicious = "DELETE FROM dim_stores WHERE 1=1;"
    is_safe, msg = clean_and_validate_sql(malicious)
    assert is_safe is False

def test_sql_sandbox_executes_valid_select():
    valid = "SELECT store_id, store_name FROM dim_stores LIMIT 3;"
    success, rows = execute_sandboxed_sql(valid)
    assert success is True
    assert len(rows) <= 3
    assert "store_id" in rows[0]

def test_end_to_end_bi_query_flow():
    query = "Liệt kê 5 cửa hàng ở khu vực TP.HCM đang có nguy cơ tồn đọng mặt hàng Thịt heo CP cao nhất chiều nay."
    res = process_bi_query(query)
    assert res["execution_success"] is True
    assert len(res["query_result"]) > 0
    assert "BÁO CÁO PHÂN TÍCH" in res["final_insight"]
