"""
LightGBM Spoilage & Demand Forecasting Model
Trains on retail inventory, sales velocity, shelf life, and weather impact
to predict 24h-48h unsold inventory and spoilage risk score (0.0 to 1.0).
"""

import os
import numpy as np
import pandas as pd
import lightgbm as lgb
from pathlib import Path

MODEL_DIR = Path(__file__).parent / "model_artifacts"
MODEL_DIR.mkdir(parents=True, exist_ok=True)
MODEL_FILE = MODEL_DIR / "spoilage_lightgbm.txt"

def generate_training_data(n_samples=2500):
    np.random.seed(42)
    # Features:
    # 1. opening_stock (kg)
    # 2. shelf_life_days (1-7 days)
    # 3. current_hour (8 to 21)
    # 4. is_weekend (0 or 1)
    # 5. is_rain_or_flood (0 or 1)
    # 6. morning_sales_velocity (units/hr)
    # 7. discount_active (0 or 1)

    opening = np.random.uniform(20.0, 100.0, n_samples)
    shelf_life = np.random.choice([1, 2, 3, 5, 7], n_samples)
    current_hour = np.random.randint(8, 22, n_samples)
    is_weekend = np.random.choice([0, 1], n_samples, p=[0.7, 0.3])
    is_rain = np.random.choice([0, 1], n_samples, p=[0.8, 0.2])
    velocity = np.random.uniform(0.5, 8.0, n_samples)
    discount = np.random.choice([0, 1], n_samples, p=[0.75, 0.25])

    # Target: unsold_units at end of day
    # Rain decreases sales (+ unsold)
    # Discount increases sales (- unsold)
    # Weekend increases sales (- unsold)
    hours_left = np.maximum(1, 22 - current_hour)
    weekend_mult = np.where(is_weekend == 1, 1.3, 1.0)
    rain_mult = np.where(is_rain == 1, 0.6, 1.0)
    discount_mult = np.where(discount == 1, 1.4, 1.0)
    expected_sales = velocity * hours_left * weekend_mult * rain_mult * discount_mult
    unsold = np.maximum(0.0, opening - expected_sales + np.random.normal(0, 3.0, n_samples))

    df = pd.DataFrame({
        "opening_stock": opening,
        "shelf_life_days": shelf_life,
        "current_hour": current_hour,
        "is_weekend": is_weekend,
        "is_rain_or_flood": is_rain,
        "morning_sales_velocity": velocity,
        "discount_active": discount,
        "unsold_units": unsold
    })
    return df

def train_spoilage_model():
    print("=" * 60)
    print("🧠 TRAINING LIGHTGBM FRESH FOOD SPOILAGE RISK MODEL")
    print("=" * 60)

    df = generate_training_data()
    feature_cols = [
        "opening_stock", "shelf_life_days", "current_hour",
        "is_weekend", "is_rain_or_flood", "morning_sales_velocity", "discount_active"
    ]
    X = df[feature_cols]
    y = df["unsold_units"]

    train_data = lgb.Dataset(X, label=y)
    params = {
        "objective": "regression",
        "metric": "rmse",
        "boosting_type": "gbdt",
        "n_estimators": 100,
        "learning_rate": 0.05,
        "num_leaves": 31,
        "verbose": -1
    }

    print("Fitting LightGBM Regressor...")
    model = lgb.train(params, train_data)

    model.save_model(str(MODEL_FILE))
    print(f"✅ Model saved to: {MODEL_FILE}")

    # Feature Importance
    importance = model.feature_importance(importance_type="gain")
    print("\n📈 Feature Importance (Gain):")
    for feat, imp in sorted(zip(feature_cols, importance), key=lambda x: x[1], reverse=True):
        print(f"  • {feat:<25}: {imp:.2f}")

    return model

def predict_spoilage(features_dict: dict) -> dict:
    """
    Inference helper for batch Airflow or real-time scoring.
    """
    if not MODEL_FILE.exists():
        train_spoilage_model()
    
    model = lgb.Booster(model_file=str(MODEL_FILE))
    feature_cols = [
        "opening_stock", "shelf_life_days", "current_hour",
        "is_weekend", "is_rain_or_flood", "morning_sales_velocity", "discount_active"
    ]
    input_data = [[features_dict.get(c, 0) for c in feature_cols]]
    pred_unsold = float(model.predict(input_data)[0])
    pred_unsold = max(0.0, pred_unsold)

    opening = features_dict.get("opening_stock", 50.0)
    risk_score = round(min(1.0, max(0.0, pred_unsold / max(1.0, opening))), 4)
    suggested_markdown = 25.0 if risk_score > 0.7 else (15.0 if risk_score > 0.4 else 0.0)

    return {
        "predicted_unsold_units": round(pred_unsold, 2),
        "spoilage_risk_score": risk_score,
        "suggested_markdown_rate": suggested_markdown
    }

if __name__ == "__main__":
    train_spoilage_model()
    # Test prediction for flood store
    sample = {
        "opening_stock": 60.0,
        "shelf_life_days": 2,
        "current_hour": 16,
        "is_weekend": 0,
        "is_rain_or_flood": 1,  # Flooded street
        "morning_sales_velocity": 1.2,
        "discount_active": 0
    }
    pred = predict_spoilage(sample)
    print(f"\n🔮 Sample Prediction for BHX Xô Viết Nghệ Tĩnh (Rain/Flood): {pred}")
