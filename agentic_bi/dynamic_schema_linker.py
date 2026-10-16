"""
Dynamic Schema Linking via Qdrant
Queries `freshflow_schema_dictionary` vector collection to retrieve
the top 3 most relevant table schemas instead of overwhelming LLM prompt with hundreds of tables.
"""

import os
from typing import Dict, Any, List
from qdrant_client import QdrantClient
from fastembed import TextEmbedding

from agentic_bi.state import BIState

QDRANT_HOST = os.environ.get("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.environ.get("QDRANT_PORT", "6333"))

DEFAULT_TABLES = [
    {"table_name": "dim_stores", "description": "Thông tin 50 chi nhánh Bách Hóa Xanh (TP.HCM)", "columns": "store_id, store_name, district, province, manager_name, phone"},
    {"table_name": "dim_products", "description": "Danh mục sản phẩm tươi sống (thịt heo, cá, rau, trái cây)", "columns": "product_id, sku, product_name, category, shelf_life_days, base_price"},
    {"table_name": "fact_realtime_inventory", "description": "Tồn kho thực tế và tỷ lệ bán (sell-through rate) 15 phút", "columns": "inventory_id, store_id, product_id, timestamp, opening_stock, units_sold, current_stock, sell_through_rate"},
    {"table_name": "ml_spoilage_predictions", "description": "Dự báo rủi ro hết hạn và đề xuất giảm giá của mô hình LightGBM", "columns": "prediction_id, store_id, product_id, predicted_unsold_units, spoilage_risk_score, suggested_markdown_rate, prediction_time"},
    {"table_name": "dead_letter_queue", "description": "Kho lưu trữ bản ghi giao dịch POS bị lỗi từ Data Quality Gate", "columns": "record_id, error_reason, payload, created_at"}
]

class DynamicSchemaLinker:
    def __init__(self):
        try:
            self.client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, timeout=0.5, check_compatibility=False)
            self.client.get_collections()
            self.embed_model = TextEmbedding("BAAI/bge-small-en-v1.5")
            self.enabled = True
        except Exception:
            self.enabled = False

    def link_schema(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        if not getattr(self, "enabled", True):
            return DEFAULT_TABLES[:top_k]
        try:
            query_vec = list(self.embed_model.embed([query]))[0].tolist()
            results = self.client.query_points(
                collection_name="freshflow_schema_dictionary",
                query=query_vec,
                limit=top_k
            )
            tables = []
            for p in results.points:
                tables.append(p.payload)
            return tables if tables else DEFAULT_TABLES[:top_k]
        except Exception as e:
            # Fallback to analytical star schema default tables
            return DEFAULT_TABLES[:top_k]

_linker = None
def get_schema_linker():
    global _linker
    if _linker is None:
        _linker = DynamicSchemaLinker()
    return _linker

def schema_linking_node(state: BIState) -> Dict[str, Any]:
    query = state.get("user_query", "")
    linker = get_schema_linker()
    relevant = linker.link_schema(query, top_k=3)

    schema_lines = ["LƯỢC ĐỒ CƠ SỞ DỮ LIỆU ĐƯỢC CHỌN LỌC (PostgreSQL):"]
    for t in relevant:
        schema_lines.append(f"- Bảng `{t['table_name']}`: {t['description']}")
        schema_lines.append(f"  Cột: {t['columns']}")

    prompt_context = "\n".join(schema_lines)
    return {
        "relevant_tables": relevant,
        "schema_prompt": prompt_context
    }
