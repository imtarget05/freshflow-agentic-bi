"""
Unit Tests for LightGBM Spoilage Prediction Model
"""

import pytest
from ml.train_spoilage_model import predict_spoilage

def test_predict_spoilage_normal():
    sample = {
        "opening_stock": 50.0,
        "shelf_life_days": 2,
        "current_hour": 14,
        "is_weekend": 0,
        "is_rain_or_flood": 0,
        "morning_sales_velocity": 3.0,
        "discount_active": 0
    }
    pred = predict_spoilage(sample)
    assert "predicted_unsold_units" in pred
    assert "spoilage_risk_score" in pred
    assert "suggested_markdown_rate" in pred
    assert 0.0 <= pred["spoilage_risk_score"] <= 1.0

def test_predict_spoilage_high_risk_flood():
    sample = {
        "opening_stock": 80.0,
        "shelf_life_days": 1,
        "current_hour": 17,
        "is_weekend": 0,
        "is_rain_or_flood": 1,  # Flood causes high risk
        "morning_sales_velocity": 0.5,
        "discount_active": 0
    }
    pred = predict_spoilage(sample)
    assert pred["spoilage_risk_score"] > 0.6
    assert pred["suggested_markdown_rate"] >= 15.0
