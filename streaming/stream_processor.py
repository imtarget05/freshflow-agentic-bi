"""
Stream Processor for FreshFlow POS Transactions
Reads events from Redpanda / Kafka topic `pos_transactions` (or simulator),
applies Pandera Data Quality Gate, and aggregates rolling sell-through rate into PostgreSQL.
"""

import os
import json
import time
import psycopg2
import sys
from pathlib import Path
from typing import Dict, Any, Optional
sys.path.append(str(Path(__file__).parent.parent))

from streaming.data_quality import DataQualityGate
from streaming.pos_simulator import generate_transaction

try:
    from kafka import KafkaConsumer
except ImportError:
    KafkaConsumer = None

NEON_CLOUD_URL = "postgresql://neondb_owner:npg_SEnl7VFHrOC6@ep-spring-lake-ao6ij6s5-pooler.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"
DATABASE_URL = os.environ.get("DATABASE_URL") or NEON_CLOUD_URL
PG_HOST = os.environ.get("POSTGRES_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5433"))
PG_USER = os.environ.get("POSTGRES_USER", "retail_admin")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "retail_password")
PG_DB = os.environ.get("POSTGRES_DB", "retail_ecosystem")

def get_db_connection():
    db_url = os.environ.get("DATABASE_URL") or DATABASE_URL or NEON_CLOUD_URL
    if db_url:
        return psycopg2.connect(db_url)
    return psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC = "pos_transactions"

def process_single_transaction(record: Dict[str, Any], quality_gate: DataQualityGate, conn):
    # 1. Data Quality Check
    is_valid, error_reason = quality_gate.validate_record(record)
    if not is_valid:
        quality_gate.route_to_dead_letter_queue(error_reason=error_reason, payload=record)
        return False

    # 2. Update PostgreSQL Fact Tables
    store_id = record["store_id"]
    product_id = record["product_id"]
    qty = record["quantity"]
    price = record["price_applied"]
    discount = record.get("discount_rate", 0.0)
    tx_id = record["transaction_id"]

    with conn.cursor() as cur:
        # Record transaction
        cur.execute("""
            INSERT INTO fact_pos_transactions (transaction_id, store_id, product_id, quantity, price_applied, discount_rate)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING;
        """, (tx_id, store_id, product_id, qty, price, discount))

        # Update real-time inventory and sell-through rate
        cur.execute("""
            UPDATE fact_realtime_inventory
            SET units_sold = units_sold + %s,
                current_stock = GREATEST(0, current_stock - %s),
                sell_through_rate = LEAST(1.0, (units_sold + %s) / NULLIF(opening_stock, 0)),
                timestamp = CURRENT_TIMESTAMP
            WHERE store_id = %s AND product_id = %s;
        """, (qty, qty, qty, store_id, product_id))

        conn.commit()
    return True

def run_stream_processor(iterations: int = 50, delay: float = 0.05):
    print("=" * 60)
    print("🌊 STARTING FRESHFLOW STREAM PROCESSOR & DATA QUALITY GATE")
    print("=" * 60)

    quality_gate = DataQualityGate()
    conn = get_db_connection()
    print("✅ Connected to PostgreSQL analytical warehouse.")

    consumer = None
    if KafkaConsumer:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="earliest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=3000
            )
            print(f"✅ Connected to Redpanda Kafka Consumer on topic `{TOPIC}`.")
        except Exception:
            consumer = None

    valid_count = 0
    corrupt_count = 0

    if consumer:
        print("Listening for Kafka stream messages...")
        count = 0
        for msg in consumer:
            record = msg.value
            success = process_single_transaction(record, quality_gate, conn)
            if success:
                valid_count += 1
            else:
                corrupt_count += 1
            count += 1
            if count >= iterations:
                break
    else:
        print(f"Streaming {iterations} simulated POS events through Data Quality Gate...")
        for _ in range(iterations):
            record = generate_transaction()
            success = process_single_transaction(record, quality_gate, conn)
            if success:
                valid_count += 1
            else:
                corrupt_count += 1
            time.sleep(delay)

    conn.close()
    print(f"\n📊 Stream Processing Summary:")
    print(f"  • Valid transactions loaded to Data Warehouse: {valid_count}")
    print(f"  • Corrupt records blocked into Dead Letter Queue: {corrupt_count}")
    print("✅ Stream Processing batch completed.")

if __name__ == "__main__":
    run_stream_processor()
