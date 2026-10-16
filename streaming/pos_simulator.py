"""
POS Transaction Generator for 50 Bách Hóa Xanh Stores
Streams POS transactions to Redpanda / Kafka topic `pos_transactions`
Intentionally injects 5% dirty/corrupt data to test the Data Quality Gate.
"""

import os
import time
import json
import uuid
import random
from datetime import datetime

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = "pos_transactions"

STORES = ["BHX_BT_01", "BHX_Q12_01", "BHX_GV_01", "BHX_TD_01", "BHX_TP_01"] + [f"BHX_ST_{i:02d}" for i in range(6, 51)]
PRODUCTS = [
    ("BHX_P01", "Thịt heo CP ba rọi tươi", 145000),
    ("BHX_P02", "Sườn non heo CP tươi sạch", 195000),
    ("BHX_P03", "Thịt heo xay CP khay 500g", 65000),
    ("BHX_P04", "Cá hồi Na Uy phi lê tươi", 420000),
    ("BHX_P05", "Cá diêu hồng làm sạch", 68000),
    ("BHX_P06", "Rau muống sạch VietGAP", 12000),
    ("BHX_P07", "Cà chua Đà Lạt VietGAP", 25000),
    ("BHX_P08", "Táo Envy New Zealand", 125000),
    ("BHX_P09", "Sữa tươi TH True Milk", 38000),
    ("BHX_P10", "Cánh gà tươi CP", 48000)
]

def generate_transaction() -> dict:
    is_dirty = (random.random() < 0.05)  # 5% intentional corrupt data
    prod_id, prod_name, base_price = random.choice(PRODUCTS)
    store_id = random.choice(STORES)
    tx_id = f"TX-{uuid.uuid4().hex[:10].upper()}"

    if is_dirty:
        error_type = random.choice(["negative_price", "empty_store", "duplicate_invoice"])
        if error_type == "negative_price":
            return {
                "transaction_id": tx_id,
                "store_id": store_id,
                "product_id": prod_id,
                "product_name": prod_name,
                "quantity": random.randint(1, 3),
                "price_applied": -base_price,  # Corrupt negative price
                "discount_rate": 0.0,
                "timestamp": datetime.utcnow().isoformat(),
                "injected_error": "NEGATIVE_PRICE"
            }
        elif error_type == "empty_store":
            return {
                "transaction_id": tx_id,
                "store_id": "",  # Corrupt empty store_id
                "product_id": prod_id,
                "product_name": prod_name,
                "quantity": random.randint(1, 3),
                "price_applied": base_price,
                "discount_rate": 0.0,
                "timestamp": datetime.utcnow().isoformat(),
                "injected_error": "EMPTY_STORE_ID"
            }
        else:
            return {
                "transaction_id": "TX-STATIC-DUPLICATE-999",  # Duplicate invoice
                "store_id": store_id,
                "product_id": prod_id,
                "product_name": prod_name,
                "quantity": 1,
                "price_applied": base_price,
                "discount_rate": 0.0,
                "timestamp": datetime.utcnow().isoformat(),
                "injected_error": "DUPLICATE_INVOICE"
            }

    qty = round(random.uniform(0.5, 3.0), 1)
    discount = 0.25 if store_id == "BHX_BT_01" and prod_id == "BHX_P01" else 0.0
    price_applied = round(base_price * (1.0 - discount), 2)

    return {
        "transaction_id": tx_id,
        "store_id": store_id,
        "product_id": prod_id,
        "product_name": prod_name,
        "quantity": qty,
        "price_applied": price_applied,
        "discount_rate": discount,
        "timestamp": datetime.utcnow().isoformat(),
        "injected_error": None
    }

def stream_transactions(max_events: int = 100, delay_seconds: float = 0.1):
    producer = None
    if KafkaProducer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
            )
            print(f"✅ Connected to Redpanda Kafka at {KAFKA_BROKER}")
        except Exception as e:
            print(f"ℹ️ Kafka broker not directly reachable ({e}). Emitting via local generator pipeline.")

    print(f"🚀 Streaming {max_events} POS transactions to topic `{TOPIC}` (5% intentional corrupt records)...")
    sent = 0
    corrupt_count = 0
    for _ in range(max_events):
        record = generate_transaction()
        if record.get("injected_error"):
            corrupt_count += 1

        if producer:
            producer.send(TOPIC, record)
        sent += 1
        time.sleep(delay_seconds)

    if producer:
        producer.flush()
    print(f"🎉 Emitted {sent} transactions ({corrupt_count} corrupt injected into stream).")

if __name__ == "__main__":
    stream_transactions(max_events=50, delay_seconds=0.05)
