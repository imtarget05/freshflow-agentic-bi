-- FreshFlow Analytical Star Schema in PostgreSQL (Port 5433)
-- Chuỗi Bách Hóa Xanh (BHX) Real-time Inventory & Spoilage Prevention

CREATE TABLE IF NOT EXISTS dim_stores (
    store_id VARCHAR(50) PRIMARY KEY,
    store_name VARCHAR(255) NOT NULL,
    district VARCHAR(100) NOT NULL,
    province VARCHAR(100) DEFAULT 'TP.HCM',
    manager_name VARCHAR(150),
    phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    sku VARCHAR(100) UNIQUE NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    shelf_life_days INT NOT NULL,
    base_price NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_pos_transactions (
    transaction_id VARCHAR(100) PRIMARY KEY,
    store_id VARCHAR(50) REFERENCES dim_stores(store_id),
    product_id VARCHAR(50) REFERENCES dim_products(product_id),
    quantity NUMERIC(10, 2) NOT NULL,
    price_applied NUMERIC(12, 2) NOT NULL,
    discount_rate NUMERIC(5, 2) DEFAULT 0.0,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_realtime_inventory (
    inventory_id SERIAL PRIMARY KEY,
    store_id VARCHAR(50) REFERENCES dim_stores(store_id),
    product_id VARCHAR(50) REFERENCES dim_products(product_id),
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    opening_stock NUMERIC(10, 2) NOT NULL,
    units_sold NUMERIC(10, 2) NOT NULL DEFAULT 0,
    current_stock NUMERIC(10, 2) NOT NULL,
    sell_through_rate NUMERIC(5, 4) NOT NULL DEFAULT 0.0,
    UNIQUE(store_id, product_id)
);

CREATE TABLE IF NOT EXISTS ml_spoilage_predictions (
    prediction_id SERIAL PRIMARY KEY,
    store_id VARCHAR(50) REFERENCES dim_stores(store_id),
    product_id VARCHAR(50) REFERENCES dim_products(product_id),
    predicted_unsold_units NUMERIC(10, 2) NOT NULL,
    spoilage_risk_score NUMERIC(5, 4) NOT NULL,
    suggested_markdown_rate NUMERIC(5, 2) NOT NULL,
    prediction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(store_id, product_id)
);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    record_id SERIAL PRIMARY KEY,
    error_reason VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Read-Only user for AI Agent Sandbox
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'bi_agent_readonly') THEN
      CREATE ROLE bi_agent_readonly WITH LOGIN PASSWORD 'readonly_password';
   END IF;
END
$$;

GRANT CONNECT ON DATABASE retail_ecosystem TO bi_agent_readonly;
GRANT USAGE ON SCHEMA public TO bi_agent_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bi_agent_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO bi_agent_readonly;
