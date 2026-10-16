# ⚡ FreshFlow Stream & Agentic BI Platform — Bách Hóa Xanh (MWG)

[![Production Quality Gate](https://github.com/imtarget05/freshflow-agentic-bi/actions/workflows/ci.yml/badge.svg)](https://github.com/imtarget05/freshflow-agentic-bi/actions)
[![Database](https://img.shields.io/badge/Database-Neon%20Serverless%20Postgres-00E599)](https://neon.tech)
[![Streaming](https://img.shields.io/badge/Streaming-Redpanda%20%2F%20Kafka-FF4655)](https://redpanda.com)
[![Data Quality](https://img.shields.io/badge/Quality%20Gate-Pandera-yellow)](https://pandera.readthedocs.io)
[![LangGraph](https://img.shields.io/badge/Agentic%20BI-LangGraph-orange)](https://github.com/langchain-ai/langgraph)

> **Real-time POS Streaming Ingestion, Pandera Data Quality Gate, PostgreSQL Star Schema Warehouse, LightGBM Spoilage Forecasting, and Self-Correcting Text-to-SQL Copilot**.  
> Simulates grocery chain operations for **Bách Hóa Xanh (MWG)** across 50 stores in Ho Chi Minh City, Vietnam.

---

## 🌟 Key Architecture & Highlights

1. **Real-time Streaming Ingestion & Pandera Data Quality Gate**:
   - Ingests streaming POS transactions with 5% simulated dirty data (negative prices, missing IDs, duplicates).
   - Pandera schema validation routes 100% of anomalous records into the **Dead Letter Queue (DLQ)**.
   - Processing freshness latency: **< 10ms**.
2. **Analytical PostgreSQL Star Schema Warehouse**:
   - `dim_stores` (50 HCMC stores), `dim_products` (Meat, Fish, Veggies, Fruits), `fact_pos_transactions`, `fact_realtime_inventory`, `ml_spoilage_predictions`.
   - Hosted on **Neon Serverless PostgreSQL (Singapore `aws-ap-southeast-1`)** for 0đ cost and zero local resource consumption.
3. **LightGBM Spoilage & Markdown Optimizer**:
   - Analyzes temperature logs, weather anomalies (floods, heavy rain), and rolling sell-through rate.
   - Recommends dynamic price markdowns (15% - 30%) to clear inventory before expiration.
4. **Self-Correcting Text-to-SQL & Contextual RAG Copilot**:
   - Translates natural Vietnamese executive questions into SQL.
   - Executes queries inside a 3-second statement timeout **Read-Only AST Sandbox**.
   - Automated self-correction loop catches syntax/schema errors and repairs queries (Accuracy increases from **72.0% -> 100.0%**).
   - Enriches quantitative SQL metrics with qualitative incident logs (power outages, weather).

---

## 🚀 Live Demo & Web App

- **Streamlit Web Application**: [https://freshflow-bi.streamlit.app](https://freshflow-bi.streamlit.app)
- **Database Endpoint**: Neon Serverless Postgres (Singapore)

---

## 📊 Benchmark Results

| Metric | Baseline (Single-Pass) | FreshFlow (Self-Correcting) | Improvement |
| :--- | :--- | :--- | :--- |
| **SQL Execution Accuracy** | 72.0% | **100.0%** | **+28.0%** 🚀 |
| **End-to-End Ingestion Latency** | — | **P50: 112ms, P95: 153ms** | Real-time |
| **Data Quality Gate Precision** | — | **100.0% (Zero dirty records in Fact)** | ✅ Perfect |

---

## 🛠️ Local Development & Testing

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run automated tests
pytest tests/ -v

# 3. Run Benchmark Runner (50 queries)
python benchmark/benchmark_runner.py

# 4. Start Streamlit UI
streamlit run streamlit_app.py
```
