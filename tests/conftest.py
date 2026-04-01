from fastapi.testclient import TestClient
from api.main import app
import pytest
import numpy as np

@pytest.fixture
def client():
    """
    Fixture: FastAPI TestClient instance

    Creates a test client for making HTTP requests to the API.
    The client simulates HTTP requests without running a real server.

    Yields:
        TestClient: Configured test client for the FastAPI app
    """
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def mock_model(mocker):
    """
    Fixture: Mock XGBoost model

    Mocks the ML model.
    Returns predictable values for testing edge cases.
    Mock behavior:
        - predict_proba() returns np.array([[0.95, 0.05]]) -> fraud probability 0.05
        - predict() returns np.array([0]) -> non-fraud label

    Args:
        mocker: pytest-mock fixture for creating mocks

    Returns:
        Mock: Mocked model object
    """
    mock = mocker.patch('api.main.ml_model')
    mock.predict_proba.return_value = np.array([[0.95, 0.05]])
    mock.predict.return_value = np.array([0])
    return mock


@pytest.fixture
def sample_valid_transaction():
    """
    Fixture: Sample valid transaction data

    Provides valid transaction data that passes all validation.
    Used as baseline data for positive test cases.

    Returns:
        dict: Valid transaction data matching TransactionRequest schema
    """
    return {
        "TX_AMOUNT": 150.50,
        "CUSTOMER_AVG_AMOUNT_WINDOW_1D": 120.0,
        "CUSTOMER_AVG_AMOUNT_WINDOW_7D": 110.5,
        "CUSTOMER_AVG_AMOUNT_WINDOW_30D": 105.3,
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D": 2.0,
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D": 8.0,
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D": 25.0,
        "TERMINAL_RISK_1DAY_WINDOW": 0.02,
        "TERMINAL_RISK_7DAY_WINDOW": 0.015,
        "TERMINAL_RISK_30DAY_WINDOW": 0.01,
        "TERMINAL_NB_TX_1DAY_WINDOW": 50.0,
        "TERMINAL_NB_TX_7DAY_WINDOW": 300.0,
        "TERMINAL_NB_TX_30DAY_WINDOW": 1200.0,
        "IS_WEEKEND": False,
        "IS_NIGHT": False
    }


@pytest.fixture
def sample_high_risk_transaction(sample_valid_transaction):
    """
    Fixture: High-risk transaction (likely fraud)

    Creates a transaction with characteristics that indicate high fraud risk.
    Used to test the fraud detection threshold logic.

    Args:
        sample_valid_transaction: Base valid transaction fixture

    Returns:
        dict: High-risk transaction data
    """
    data = sample_valid_transaction.copy()
    data.update({
        "TX_AMOUNT": 5000.00,
        "IS_WEEKEND": True,
        "IS_NIGHT": True
    })
    return data


@pytest.fixture
def sample_invalid_transactions():
    """
    Fixture: Dictionary of invalid transaction cases

    Provides various invalid data scenarios for testing validation logic.
    Each key represents a different type of validation error.

    Returns:
        dict: Dictionary with keys as error types and values as invalid data
    """
    return {
        "missing_required_field": {
            "CUSTOMER_AVG_AMOUNT_WINDOW_1D": 120.0,
            "IS_WEEKEND": False,
            "IS_NIGHT": False
        },
        "negative_amount": {
            "TX_AMOUNT": -100.50,
            "IS_WEEKEND": False,
            "IS_NIGHT": False
        },
        "invalid_risk_range": {
            "TX_AMOUNT": 150.50,
            "TERMINAL_RISK_1DAY_WINDOW": 1.5,
            "IS_WEEKEND": False,
            "IS_NIGHT": False
        },
        "negative_count": {
            "TX_AMOUNT": 150.50,
            "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D": -5.0,
            "IS_WEEKEND": False,
            "IS_NIGHT": False
        }
    }
