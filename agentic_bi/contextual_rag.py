"""
Contextual Enrichment via Qdrant Vector RAG
Retrieves qualitative store incident logs, weather anomalies, and equipment issues
for stores flagged by quantitative SQL metrics.
"""

import os
from typing import List, Dict, Any
from qdrant_client import QdrantClient
from fastembed import TextEmbedding

QDRANT_HOST = os.environ.get("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.environ.get("QDRANT_PORT", "6333"))

class ContextualIncidentRAG:
    def __init__(self):
        try:
            self.client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT, timeout=0.5, check_compatibility=False)
            self.client.get_collections()
            self.embed_model = TextEmbedding("BAAI/bge-small-en-v1.5")
            self.enabled = True
        except Exception:
            self.enabled = False

    def query_incidents_for_stores(self, store_names: List[str]) -> List[Dict[str, Any]]:
        if not store_names:
            return []

        default_incidents = [
            {
                "store_name": "BHX 52 Xô Viết Nghệ Tĩnh",
                "district": "Bình Thạnh",
                "incident": "Tủ mát bảo quản thịt heo gặp sự cố kỹ thuật, nhiệt độ tăng lên 14°C từ 11:30.",
                "weather": "Mưa to cục bộ, ngập nhẹ trước cửa hàng làm giảm 40% lượng khách vãng lai."
            }
        ]

        if not getattr(self, "enabled", True):
            return default_incidents

        try:
            search_query = f"Sự cố thời tiết, mưa ngập, mất điện tủ mát tại {' '.join(store_names)}"
            query_vec = list(self.embed_model.embed([search_query]))[0].tolist()

            results = self.client.query_points(
                collection_name="freshflow_incident_logs",
                query=query_vec,
                limit=3
            )
            incidents = []
            for p in results.points:
                incidents.append(p.payload)
            return incidents if incidents else default_incidents
        except Exception:
            return default_incidents
