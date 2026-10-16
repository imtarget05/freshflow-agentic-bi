"""
State Definition for Agentic BI & Self-Correcting Text-to-SQL
"""

from typing import List, Dict, Any, Optional
from typing_extensions import TypedDict

class BIState(TypedDict):
    user_query: str
    relevant_tables: List[Dict[str, Any]]
    schema_prompt: str
    generated_sql: str
    sql_error: Optional[str]
    retry_count: int
    execution_success: bool
    query_result: Optional[List[Dict[str, Any]]]
    rag_context: Optional[List[Dict[str, Any]]]
    final_insight: str
