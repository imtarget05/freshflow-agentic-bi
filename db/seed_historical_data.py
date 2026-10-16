"""
Seed Historical Star Schema Data for FreshFlow (Bách Hóa Xanh)
Loads:
- 50 BHX Stores across HCMC districts
- Fresh food products (Meat, Fish, Veggies, Fruits)
- Current Real-time Inventory levels
- Baseline ML Spoilage Predictions (with special case for BHX Xô Viết Nghệ Tĩnh)
- Schema Dictionary and Store Incidents indexed into Qdrant
"""

import os
import random
import psycopg2
from psycopg2.extras import execute_values
from qdrant_client import QdrantClient
from qdrant_client.http import models
from fastembed import TextEmbedding

PG_HOST = os.environ.get("POSTGRES_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5433"))
PG_USER = os.environ.get("POSTGRES_USER", "retail_admin")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "retail_password")
PG_DB = os.environ.get("POSTGRES_DB", "retail_ecosystem")

QDRANT_HOST = os.environ.get("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.environ.get("QDRANT_PORT", "6333"))

DISTRICTS = ["Bình Thạnh", "Gò Vấp", "Quận 12", "Thủ Đức", "Tân Phú", "Bình Tân", "Quận 7", "Quận 8", "Hóc Môn", "Củ Chi"]

PRODUCTS_DATA = [
    ("BHX_P01", "PORK-CP-BA-ROI", "Thịt heo CP ba rọi tươi", "thit_tuoi", 2, 145000),
    ("BHX_P02", "PORK-CP-SUON-NON", "Sườn non heo CP tươi sạch", "thit_tuoi", 2, 195000),
    ("BHX_P03", "PORK-CP-THIT-XAY", "Thịt heo xay CP khay 500g", "thit_tuoi", 2, 65000),
    ("BHX_P04", "FISH-SALMON-PHI-LE", "Cá hồi Na Uy phi lê tươi", "thuy_hai_san", 2, 420000),
    ("BHX_P05", "FISH-CA-DIEU-HONG", "Cá diêu hồng làm sạch nguyên con", "thuy_hai_san", 2, 68000),
    ("BHX_P06", "VEG-RAU-MUONG", "Rau muống sạch VietGAP 500g", "rau_cu", 3, 12000),
    ("BHX_P07", "VEG-CA-CHUA", "Cà chua Đà Lạt VietGAP 1kg", "rau_cu", 5, 25000),
    ("BHX_P08", "FRUIT-TAO-ENVY", "Táo Envy New Zealand size 80", "trai_cay", 7, 125000),
    ("BHX_P09", "MILK-TH-TRUE", "Sữa tươi TH True Milk 1L không đường", "thuc_pham_ngan_ngay", 14, 38000),
    ("BHX_P10", "CHICKEN-CP-CANH", "Cánh gà tươi CP khay 500g", "thit_tuoi", 2, 48000)
]

STORE_INCIDENTS = [
    {
        "store_id": "BHX_BT_01",
        "store_name": "BHX 52 Xô Viết Nghệ Tĩnh, Bình Thạnh",
        "district": "Bình Thạnh",
        "incident": "Khu vực Bình Thạnh mưa to dông lốc, đường Xô Viết Nghệ Tĩnh ngập sâu lúc 16h00 làm lượng khách ghé cửa hàng giảm hơn 40% so với ngày thường.",
        "weather": "Mưa to cục bộ, ngập úng"
    },
    {
        "store_id": "BHX_Q12_01",
        "store_name": "BHX 145 Nguyễn Ảnh Thủ, Quận 12",
        "district": "Quận 12",
        "incident": "Cửa hàng BHX Nguyễn Ảnh Thủ mất điện tủ mát 2 tiếng sáng nay từ 08h30 đến 10h30 trước khi máy phát điện dự phòng hoạt động, nhiệt độ bảo quản tăng nhẹ.",
        "weather": "Nắng gắt, nhiệt độ 35 độ C"
    },
    {
        "store_id": "BHX_GV_01",
        "store_name": "BHX 320 Lê Đức Thọ, Gò Vấp",
        "district": "Gò Vấp",
        "incident": "Giao thông tắc nghẽn do thi công cầu vượt, xe tải trung chuyển rau củ đến trễ 45 phút.",
        "weather": "Thời tiết bình thường"
    }
]

SCHEMA_DICTIONARY = [
    {
        "table_name": "dim_stores",
        "description": "Lưu danh mục 50 cửa hàng Bách Hóa Xanh tại TP.HCM, thông tin quận huyện, tên cửa hàng trưởng và số điện thoại liên hệ.",
        "columns": "store_id, store_name, district, province, manager_name, phone"
    },
    {
        "table_name": "dim_products",
        "description": "Lưu danh mục hàng hóa thực phẩm tươi sống, mã SKU, tên hàng (Thịt heo CP, Cá hồi, Rau muống), thời hạn sử dụng (shelf_life_days) và giá bán lẻ niêm yết.",
        "columns": "product_id, sku, product_name, category, shelf_life_days, base_price"
    },
    {
        "table_name": "fact_realtime_inventory",
        "description": "Lưu chỉ số tồn kho thời gian thực theo từng cửa hàng và mặt hàng, tồn đầu ngày (opening_stock), đã bán (units_sold), tồn hiện tại (current_stock), tốc độ tiêu thụ (sell_through_rate).",
        "columns": "store_id, product_id, opening_stock, units_sold, current_stock, sell_through_rate, timestamp"
    },
    {
        "table_name": "ml_spoilage_predictions",
        "description": "Bảng dự báo bằng mô hình học máy LightGBM: số lượng hàng dự báo ế ẩm vào cuối ngày (predicted_unsold_units), chỉ số rủi ro hủy hàng (spoilage_risk_score từ 0 đến 1), và tỷ lệ chiết khấu giảm giá đề xuất (suggested_markdown_rate, ví dụ 25%).",
        "columns": "store_id, product_id, predicted_unsold_units, spoilage_risk_score, suggested_markdown_rate, prediction_time"
    },
    {
        "table_name": "dead_letter_queue",
        "description": "Kho lưu trữ các bản ghi giao dịch POS bị lỗi do Data Quality Gate chặn lại (giá âm, trùng lặp mã đơn, thiếu store_id) để phục vụ giám sát.",
        "columns": "record_id, error_reason, payload, created_at"
    }
]

def main():
    print(f"Connecting to PostgreSQL at {PG_HOST}:{PG_PORT} for FreshFlow...")
    conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASS, dbname=PG_DB)
    
    # 1. Execute DDL
    with open(os.path.join(os.path.dirname(__file__), "init_schema.sql"), "r") as f:
        ddl = f.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()
    print("✅ FreshFlow Star Schema initialized.")

    # 2. Seed Stores (50 BHX Stores)
    stores = [
        ("BHX_BT_01", "BHX 52 Xô Viết Nghệ Tĩnh", "Bình Thạnh", "TP.HCM", "Nguyễn Văn Hùng", "0908111222"),
        ("BHX_Q12_01", "BHX 145 Nguyễn Ảnh Thủ", "Quận 12", "TP.HCM", "Trần Đình Trọng", "0908333444"),
        ("BHX_GV_01", "BHX 320 Lê Đức Thọ", "Gò Vấp", "TP.HCM", "Lê Văn Tám", "0908555666"),
        ("BHX_TD_01", "BHX 78 Đặng Văn Bi", "Thủ Đức", "TP.HCM", "Phạm Quốc Dũng", "0908777888"),
        ("BHX_TP_01", "BHX 210 Lũy Bán Bích", "Tân Phú", "TP.HCM", "Võ Thị Sáu", "0908999000")
    ]
    for i in range(6, 51):
        dist = random.choice(DISTRICTS)
        stores.append((f"BHX_ST_{i:02d}", f"BHX Chi nhánh {i} {dist}", dist, "TP.HCM", f"Quản lý {i}", f"0909{i:06d}"))

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO dim_stores (store_id, store_name, district, province, manager_name, phone)
            VALUES %s ON CONFLICT (store_id) DO NOTHING;
        """, stores)

        # Products
        execute_values(cur, """
            INSERT INTO dim_products (product_id, sku, product_name, category, shelf_life_days, base_price)
            VALUES %s ON CONFLICT (product_id) DO NOTHING;
        """, PRODUCTS_DATA)

        # Realtime Inventory
        inv_rows = []
        pred_rows = []
        for s in stores:
            for p in PRODUCTS_DATA:
                opening = random.randint(30, 80)
                sold = random.randint(5, int(opening * 0.7))
                curr = opening - sold
                sell_through = round(sold / opening, 4)

                # Special case matching blueprint for BHX 52 Xô Viết Nghệ Tĩnh (Thịt heo CP)
                if s[0] == "BHX_BT_01" and p[0] == "BHX_P01":
                    opening = 60
                    sold = 15  # Poor sales due to flood
                    curr = 45
                    sell_through = 0.25
                    unsold = 18.5
                    risk_score = 0.8800
                    markdown = 25.0
                elif s[0] == "BHX_Q12_01" and p[0] == "BHX_P01":
                    unsold = 15.0
                    risk_score = 0.8200
                    markdown = 20.0
                else:
                    unsold = round(random.uniform(2.0, 12.0), 1)
                    risk_score = round(random.uniform(0.15, 0.75), 4)
                    markdown = 15.0 if risk_score > 0.6 else 0.0

                inv_rows.append((s[0], p[0], opening, sold, curr, sell_through))
                pred_rows.append((s[0], p[0], unsold, risk_score, markdown))

        execute_values(cur, """
            INSERT INTO fact_realtime_inventory (store_id, product_id, opening_stock, units_sold, current_stock, sell_through_rate)
            VALUES %s ON CONFLICT (store_id, product_id) DO UPDATE SET
                opening_stock = EXCLUDED.opening_stock,
                units_sold = EXCLUDED.units_sold,
                current_stock = EXCLUDED.current_stock,
                sell_through_rate = EXCLUDED.sell_through_rate;
        """, inv_rows)

        execute_values(cur, """
            INSERT INTO ml_spoilage_predictions (store_id, product_id, predicted_unsold_units, spoilage_risk_score, suggested_markdown_rate)
            VALUES %s ON CONFLICT (store_id, product_id) DO UPDATE SET
                predicted_unsold_units = EXCLUDED.predicted_unsold_units,
                spoilage_risk_score = EXCLUDED.spoilage_risk_score,
                suggested_markdown_rate = EXCLUDED.suggested_markdown_rate;
        """, pred_rows)

        conn.commit()
    conn.close()
    print("✅ Seeded 50 stores, 10 products, and 500 realtime inventory & ML predictions into PostgreSQL.")

    # 3. Index Schema Dictionary & Incidents into Qdrant
    print("Indexing Schema Dictionary and Store Incidents into Qdrant...")
    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    embed_model = TextEmbedding("BAAI/bge-small-en-v1.5")

    # Collection 1: Schema Dictionary for Dynamic Schema Linking
    client.recreate_collection(
        collection_name="freshflow_schema_dictionary",
        vectors_config=models.VectorParams(size=384, distance=models.Distance.COSINE)
    )
    schema_texts = [f"Table: {s['table_name']}. Description: {s['description']}. Columns: {s['columns']}" for s in SCHEMA_DICTIONARY]
    schema_vecs = list(embed_model.embed(schema_texts))
    schema_points = [
        models.PointStruct(id=i, vector=schema_vecs[i].tolist(), payload=SCHEMA_DICTIONARY[i])
        for i in range(len(SCHEMA_DICTIONARY))
    ]
    client.upsert(collection_name="freshflow_schema_dictionary", points=schema_points)
    print("✅ Indexed Schema Dictionary into Qdrant `freshflow_schema_dictionary`.")

    # Collection 2: Store Incidents for Contextual RAG
    client.recreate_collection(
        collection_name="freshflow_incident_logs",
        vectors_config=models.VectorParams(size=384, distance=models.Distance.COSINE)
    )
    inc_texts = [f"Store: {inc['store_name']}. District: {inc['district']}. Incident: {inc['incident']}. Weather: {inc['weather']}" for inc in STORE_INCIDENTS]
    inc_vecs = list(embed_model.embed(inc_texts))
    inc_points = [
        models.PointStruct(id=i, vector=inc_vecs[i].tolist(), payload=STORE_INCIDENTS[i])
        for i in range(len(STORE_INCIDENTS))
    ]
    client.upsert(collection_name="freshflow_incident_logs", points=inc_points)
    print("✅ Indexed Store Incidents into Qdrant `freshflow_incident_logs`.")

if __name__ == "__main__":
    main()
