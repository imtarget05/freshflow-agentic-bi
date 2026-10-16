"""
FreshFlow Stream & Agentic BI Platform — Bách Hóa Xanh (MWG)
Standalone Production Streamlit Web Application
"""

import os
import sys
import json
import time
import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# Setup paths
APP_DIR = Path(__file__).resolve().parent
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

# Default Neon Serverless PostgreSQL (Singapore)
NEON_CLOUD_URL = "postgresql://neondb_owner:npg_SEnl7VFHrOC6@ep-spring-lake-ao6ij6s5-pooler.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    try:
        if hasattr(st, "secrets") and "DATABASE_URL" in st.secrets:
            DATABASE_URL = st.secrets["DATABASE_URL"]
    except Exception:
        pass
if not DATABASE_URL:
    DATABASE_URL = NEON_CLOUD_URL

os.environ["DATABASE_URL"] = DATABASE_URL

from agentic_bi.graph import process_bi_query
from streaming.pos_simulator import generate_transaction
from streaming.stream_processor import process_single_transaction
from streaming.data_quality import DataQualityGate
from airflow.dags.freshflow_hourly_dag import run_hourly_pipeline

st.set_page_config(
    page_title="FreshFlow Stream & Agentic BI (BHX)",
    page_icon="⚡",
    layout="wide"
)

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

st.title("⚡ FreshFlow Stream & Agentic BI Platform — Bách Hóa Xanh (MWG)")
st.caption("Nền tảng Streaming Data Pipeline & Trợ lý AI Phân tích Tồn kho Thực phẩm Tươi sống (0đ Cloud Production)")

col_left, col_right = st.columns([1, 1], gap="large")

def get_db_metrics():
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_pos_transactions;")
            tx_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM dead_letter_queue;")
            dlq_count = cur.fetchone()[0]
            cur.execute("SELECT error_reason, COUNT(*) FROM dead_letter_queue GROUP BY error_reason;")
            dlq_breakdown = cur.fetchall()
            cur.execute("SELECT record_id, error_reason, payload, created_at FROM dead_letter_queue ORDER BY record_id DESC LIMIT 5;")
            recent_dlq = cur.fetchall()
        conn.close()
        return tx_count, dlq_count, dlq_breakdown, recent_dlq
    except Exception as e:
        return 0, 0, [], []

# ==============================================================================
# LEFT PANE: Real-time Data Pipeline Monitor
# ==============================================================================
with col_left:
    st.subheader("📡 Real-time Data Pipeline Monitor")
    st.write("Luồng Streaming Ingestion (Redpanda / Kafka) & Data Quality Gate")

    tx_count, dlq_count, dlq_breakdown, recent_dlq = get_db_metrics()

    m1, m2, m3 = st.columns(3)
    m1.metric("Giao dịch POS Hợp lệ", f"{tx_count:,}")
    m2.metric("Chặn bởi Data Quality", f"{dlq_count:,}", delta=f"{dlq_count} lỗi", delta_color="inverse")
    m3.metric("Số cửa hàng hoạt động", "50 BHX")

    st.markdown("#### ⚡ Thao tác Dữ liệu Thời gian thực:")
    b1, b2 = st.columns(2)
    if b1.button("▶️ Bắn 20 Giao dịch POS Mẫu (kèm 5% rác)", use_container_width=True):
        gate = DataQualityGate()
        conn = get_db_connection()
        for _ in range(20):
            tx = generate_transaction()
            process_single_transaction(tx, gate, conn)
        conn.close()
        st.success("Đã bắn và xử lý 20 giao dịch thành công!")
        st.rerun()

    if b2.button("⏱️ Kích hoạt Airflow Hourly Pipeline", use_container_width=True):
        run_hourly_pipeline()
        st.success("Airflow Hourly DAG đã hoàn thành 4 task!")
        st.rerun()

    st.markdown("#### 🚨 Bảng Dead Letter Queue (Dữ liệu lỗi bị chặn):")
    if recent_dlq:
        df_dlq = pd.DataFrame(recent_dlq, columns=["ID", "Lý do lỗi", "Payload", "Thời gian"])
        st.dataframe(df_dlq[["ID", "Lý do lỗi", "Thời gian"]], use_container_width=True)
    else:
        st.info("Hiện chưa có bản ghi lỗi nào trong Dead Letter Queue.")

# ==============================================================================
# RIGHT PANE: Agentic BI Copilot
# ==============================================================================
with col_right:
    st.subheader("🤖 Agentic BI Copilot (Text-to-SQL & RAG)")
    st.write("Trợ lý Giám đốc vùng hỏi đáp tiếng Việt tự nhiên & tự động sinh SQL an toàn")

    default_query = "Liệt kê 5 cửa hàng ở khu vực TP.HCM đang có nguy cơ tồn đọng mặt hàng Thịt heo CP cao nhất chiều nay, nguyên nhân do đâu và đề xuất mức giảm giá xả hàng?"
    query = st.text_area("Câu hỏi phân tích nghiệp vụ:", value=default_query, height=100)

    if st.button("🚀 Chạy Phân tích Agentic BI", type="primary", use_container_width=True):
        with st.spinner("Đang chạy Dynamic Schema Linking -> Sinh SQL -> Sandbox -> RAG -> Báo cáo..."):
            res = process_bi_query(query)

        st.success("Phân tích hoàn tất!")

        # Display generated SQL
        with st.expander("🔍 Câu truy vấn SQL do AI tự sinh (Read-Only Sandbox)", expanded=True):
            st.code(res.get("generated_sql", ""), language="sql")
            retries = res.get("retry_count", 0)
            if retries > 0:
                st.warning(f"Đã kích hoạt Self-Correction Loop: Tự sửa lỗi sau {retries} lần thử!")
            else:
                st.info("Thực thi thành công ngay lần đầu (Zero Syntax Error)!")

        # Chart visualization if data exists
        rows = res.get("query_result")
        if rows:
            df_rows = pd.DataFrame(rows)
            if "spoilage_risk_score" in df_rows.columns and "store_name" in df_rows.columns:
                st.markdown("#### 📊 Biểu đồ Nguy cơ Hủy hàng theo Cửa hàng:")
                df_rows["spoilage_risk_score"] = df_rows["spoilage_risk_score"].astype(float)
                fig = px.bar(
                    df_rows,
                    x="store_name",
                    y="spoilage_risk_score",
                    color="spoilage_risk_score",
                    color_continuous_scale="Reds",
                    title="Chỉ số Nguy cơ Hủy hàng (Spoilage Risk Score)",
                    labels={"spoilage_risk_score": "Nguy cơ hủy hàng", "store_name": "Cửa hàng"}
                )
                fig.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=20))
                st.plotly_chart(fig, use_container_width=True)

        # Markdown Executive Report
        st.markdown("#### 📑 Báo cáo Phân tích & Đề xuất Chiết khấu:")
        st.markdown(res.get("final_insight", ""))
