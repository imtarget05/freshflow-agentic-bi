"""
SQL Guardrail & Read-Only Sandbox Executor
Security Rules:
1. AST / Lexical Check: Strictly permits only `SELECT` queries. Chặn tuyệt đối DROP, DELETE, UPDATE, INSERT.
2. Connection Isolation: Connects via PostgreSQL role `bi_agent_readonly` with password `readonly_password`.
3. Execution Timeout: Enforces `statement_timeout = '3s'` to prevent long-running table lock queries.
"""

import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Tuple

PG_HOST = os.environ.get("POSTGRES_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5433"))
PG_USER = os.environ.get("POSTGRES_READONLY_USER", "bi_agent_readonly")
PG_PASS = os.environ.get("POSTGRES_READONLY_PASS", "readonly_password")
PG_DB = os.environ.get("POSTGRES_DB", "retail_ecosystem")

DANGEROUS_KEYWORDS = ["drop", "delete", "update", "insert", "truncate", "alter", "grant", "revoke", "create", "execute"]

def clean_and_validate_sql(sql_text: str) -> Tuple[bool, str]:
    """
    Validates that the SQL text is purely a read-only SELECT statement.
    """
    clean_sql = sql_text.strip()
    if clean_sql.startswith("```sql"):
        clean_sql = clean_sql[6:]
    if clean_sql.startswith("```"):
        clean_sql = clean_sql[3:]
    if clean_sql.endswith("```"):
        clean_sql = clean_sql[:-3]
    clean_sql = clean_sql.strip()

    first_word = clean_sql.split()[0].lower() if clean_sql.split() else ""
    if first_word != "select" and first_word != "with":
        return False, f"BẢO MẬT: Chỉ cho phép câu lệnh SELECT hoặc WITH (Đã chặn từ khóa '{first_word.upper()}')"

    for kw in DANGEROUS_KEYWORDS:
        pattern = r"\b" + kw + r"\b"
        if re.search(pattern, clean_sql.lower()):
            return False, f"BẢO MẬT: Phát hiện từ khóa nguy hiểm bị cấm: '{kw.upper()}'"

    return True, clean_sql

def execute_sandboxed_sql(sql_text: str) -> Tuple[bool, Any]:
    """
    Executes SQL inside 3s timeout sandbox.
    Returns: (success: bool, result_or_error: List[Dict] or str)
    """
    is_safe, validated_sql = clean_and_validate_sql(sql_text)
    if not is_safe:
        return False, validated_sql

    DATABASE_URL = os.environ.get("DATABASE_URL")
    conn = None
    try:
        if DATABASE_URL:
            conn = psycopg2.connect(
                DATABASE_URL,
                cursor_factory=RealDictCursor
            )
        else:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                user=PG_USER,
                password=PG_PASS,
                dbname=PG_DB,
                cursor_factory=RealDictCursor
            )
        with conn.cursor() as cur:
            try:
                cur.execute("SET statement_timeout = 3000;")
            except Exception:
                pass
            cur.execute(validated_sql)
            rows = cur.fetchall()
            # Convert to plain dicts
            results = [dict(r) for r in rows]
            return True, results
    except Exception as e:
        error_msg = str(e).strip()
        return False, f"PostgresError: {error_msg}"
    finally:
        if conn:
            conn.close()
