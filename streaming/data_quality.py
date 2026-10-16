"""
Data Quality Gate using Pandera for FreshFlow POS Transactions
Validates transactions in real time:
- Chặn dữ liệu rác (giá âm, mã cửa hàng rỗng, số lượng <= 0, trùng lặp mã đơn)
- Đẩy bản ghi lỗi sang dead_letter_queue và lưu vào PostgreSQL.
"""

import os
import json
import psycopg2
import pandas as pd
import pandera as pa
from typing import Dict, Any, Tuple, List

PG_HOST = os.environ.get("POSTGRES_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5433"))
PG_USER = os.environ.get("POSTGRES_USER", "retail_admin")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "retail_password")
PG_DB = os.environ.get("POSTGRES_DB", "retail_ecosystem")

class DataQualityGate:
    def __init__(self):
        self.seen_transaction_ids = set()

    def validate_record(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Validates a single POS transaction record.
        Returns: (is_valid: bool, error_reason: str)
        """
        tx_id = record.get("transaction_id")
        store_id = record.get("store_id")
        price = record.get("price_applied", 0)
        quantity = record.get("quantity", 0)

        # 1. Missing or empty store_id
        if not store_id or str(store_id).strip() == "":
            return False, "INVALID_STORE_ID: Mã cửa hàng bị rỗng hoặc thiếu"

        # 2. Negative or zero price
        if price is None or price <= 0:
            return False, f"NEGATIVE_OR_ZERO_PRICE: Giá bán không hợp lệ ({price}đ)"

        # 3. Invalid quantity
        if quantity is None or quantity <= 0:
            return False, f"INVALID_QUANTITY: Số lượng mua không hợp lệ ({quantity})"

        # 4. Duplicate invoice ID
        if tx_id in self.seen_transaction_ids:
            return False, f"DUPLICATE_TRANSACTION_ID: Mã hóa đơn {tx_id} đã tồn tại trong phiên"
        
        if tx_id:
            self.seen_transaction_ids.add(tx_id)

        return True, "VALID"

    def route_to_dead_letter_queue(self, error_reason: str, payload: Dict[str, Any]):
        """
        Persists corrupted record into dead_letter_queue in PostgreSQL.
        """
        try:
            conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB)
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO dead_letter_queue (error_reason, payload, created_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP);
                """, (error_reason, json.dumps(payload, ensure_ascii=False)))
                conn.commit()
            conn.close()
            print(f"🚨 [DLQ ALERT] Blocked corrupt record: {error_reason}")
        except Exception as e:
            print(f"Error writing to DLQ: {e}")
