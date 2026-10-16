"""
Unit Tests for Data Quality Gate
"""

import pytest
from streaming.data_quality import DataQualityGate

def test_valid_record():
    gate = DataQualityGate()
    record = {
        "transaction_id": "TX-VAL-001",
        "store_id": "BHX_BT_01",
        "price_applied": 145000,
        "quantity": 2.0
    }
    is_valid, reason = gate.validate_record(record)
    assert is_valid is True
    assert reason == "VALID"

def test_negative_price_blocked():
    gate = DataQualityGate()
    record = {
        "transaction_id": "TX-NEG-001",
        "store_id": "BHX_BT_01",
        "price_applied": -50000,
        "quantity": 1.0
    }
    is_valid, reason = gate.validate_record(record)
    assert is_valid is False
    assert "NEGATIVE_OR_ZERO_PRICE" in reason

def test_empty_store_id_blocked():
    gate = DataQualityGate()
    record = {
        "transaction_id": "TX-EMPTY-001",
        "store_id": "",
        "price_applied": 100000,
        "quantity": 1.0
    }
    is_valid, reason = gate.validate_record(record)
    assert is_valid is False
    assert "INVALID_STORE_ID" in reason

def test_duplicate_transaction_id_blocked():
    gate = DataQualityGate()
    record = {
        "transaction_id": "TX-DUP-001",
        "store_id": "BHX_BT_01",
        "price_applied": 100000,
        "quantity": 1.0
    }
    is_valid, _ = gate.validate_record(record)
    assert is_valid is True
    # Second time should fail
    is_valid2, reason2 = gate.validate_record(record)
    assert is_valid2 is False
    assert "DUPLICATE_TRANSACTION_ID" in reason2
