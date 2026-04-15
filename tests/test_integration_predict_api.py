import numpy as np
import pytest
from fastapi import status


class TestPredictEndpoint:
    """Test suite for /predict endpoint integration tests"""

    def test_predict_success_non_fraud(
        self, client, sample_valid_transaction, mock_model
    ):
        """
        Test: Successful prediction for non-fraud transaction

        Verifies that the endpoint returns correct response for valid input.
        Mock model returns fraud probability of 0.05 (non-fraud).

        Given: Valid transaction data and mocked model returning 0.05 fraud prob
        When: POST request is made to /predict endpoint
        Then:
            - Response status is 200 OK
            - is_fraud is False
            - fraud_probability is 0.05
            - model_version and timestamp are present
        """
        mock_model.predict_proba.return_value = np.array([[0.95, 0.05]])
        mock_model.predict.return_value = np.array([0])

        response = client.post("/predict", json=sample_valid_transaction)

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        assert data["is_fraud"] is False
        assert data["fraud_probability"] == 0.05
        assert "model_version" in data
        assert "timestamp" in data
        assert isinstance(data["fraud_probability"], (int, float))

    def test_predict_success_fraud(self, client, sample_valid_transaction, mock_model):
        """
        Test: Successful prediction for fraud transaction

        Verifies fraud detection when probability >= 0.5 threshold.
        Mock model returns fraud probability of 0.7 (fraud).

        Given: Valid transaction data and mocked model returning 0.7 fraud prob
        When: POST request is made to /predict endpoint
        Then:
            - Response status is 200 OK
            - is_fraud is True (since 0.7 >= 0.5)
            - fraud_probability is 0.7
        """
        mock_model.predict_proba.return_value = np.array([[0.3, 0.7]])
        mock_model.predict.return_value = np.array([1])

        response = client.post("/predict", json=sample_valid_transaction)

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        assert data["is_fraud"] is True
        assert data["fraud_probability"] == 0.7
        assert "model_version" in data

    def test_predict_threshold_boundary(
        self, client, sample_valid_transaction, mock_model
    ):
        """
        Test: Prediction with fraud probability exactly 0.5

        Verifies boundary condition for fraud detection threshold.
        Code logic: is_fraud = bool(fraud_probability >= 0.5)
        With fraud_probability=0.5, is_fraud should be True.

        Given: Valid data and mocked model returning exactly 0.5 fraud prob
        When: POST request is made to /predict endpoint
        Then:
            - is_fraud is True (>= boundary)
            - fraud_probability is 0.5
        """
        mock_model.predict_proba.return_value = np.array([[0.5, 0.5]])
        mock_model.predict.return_value = np.array([1])

        response = client.post("/predict", json=sample_valid_transaction)

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        assert data["is_fraud"] is True
        assert data["fraud_probability"] == 0.5

    def test_predict_just_below_threshold(
        self, client, sample_valid_transaction, mock_model
    ):
        """
        Test: Prediction with fraud probability just below 0.5

        Verifies boundary condition for non-fraud detection.
        Edge case: 0.4999 should be non-fraud.

        Given: Valid data and mocked model returning 0.4999 fraud prob
        When: POST request is made to /predict endpoint
        Then:
            - is_fraud is False (< threshold)
            - fraud_probability is 0.4999
        """
        mock_model.predict_proba.return_value = np.array([[0.5001, 0.4999]])
        mock_model.predict.return_value = np.array([0])

        response = client.post("/predict", json=sample_valid_transaction)

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        assert data["is_fraud"] is False
        assert abs(data["fraud_probability"] - 0.4999) < 0.0001

    def test_predict_missing_required_field(self, client):
        """
        Test: ValidationError when required field is missing

        Verifies that the endpoint rejects incomplete requests.
        Pydantic validation occurs before model prediction.
        FastAPI returns 422 for request validation errors.

        Given: Request data missing TX_AMOUNT field
        When: POST request is made to /predict endpoint
        Then:
            - Response status is 422 Unprocessable Entity
            - Error message indicates validation failure
        """
        invalid_data = {
            "CUSTOMER_AVG_AMOUNT_WINDOW_1D": 120.0,
            "IS_WEEKEND": False,
            "IS_NIGHT": False,
        }

        response = client.post("/predict", json=invalid_data)

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        data = response.json()

        assert "detail" in data

    def test_predict_negative_amount(self, client, sample_valid_transaction):
        """
        Test: ValidationError for negative transaction amount

        Verifies that the endpoint rejects negative transaction amounts.
        Constraint: TX_AMOUNT must be > 0
        FastAPI returns 422 for request validation errors.

        Given: Request data with negative TX_AMOUNT
        When: POST request is made to /predict endpoint
        Then:
            - Response status is 422 Unprocessable Entity
            - Error message indicates validation failure
        """
        invalid_data = sample_valid_transaction.copy()
        invalid_data["TX_AMOUNT"] = -100.50

        response = client.post("/predict", json=invalid_data)

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        data = response.json()

        assert "detail" in data

    def test_predict_invalid_risk_range(self, client, sample_valid_transaction):
        """
        Test: ValidationError for terminal risk outside valid range

        Verifies that terminal risk values must be in [0, 1] range.
        Constraint: TERMINAL_RISK_* must be between 0 and 1
        FastAPI returns 422 for request validation errors.

        Given: Request data with TERMINAL_RISK_1DAY_WINDOW = 1.5
        When: POST request is made to /predict endpoint
        Then:
            - Response status is 422 Unprocessable Entity
            - Error message indicates validation failure
        """
        invalid_data = sample_valid_transaction.copy()
        invalid_data["TERMINAL_RISK_1DAY_WINDOW"] = 1.5

        response = client.post("/predict", json=invalid_data)

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        data = response.json()

        assert "detail" in data

    def test_predict_negative_count(self, client, sample_valid_transaction):
        """
        Test: ValidationError for negative count values

        Verifies that count fields cannot be negative.
        Constraint: *_WINDOW count fields must be >= 0
        FastAPI returns 422 for request validation errors.

        Given: Request data with negative customer transaction count
        When: POST request is made to /predict endpoint
        Then:
            - Response status is 422 Unprocessable Entity
            - Error message indicates validation failure
        """
        invalid_data = sample_valid_transaction.copy()
        invalid_data["CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D"] = -5.0

        response = client.post("/predict", json=invalid_data)

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        data = response.json()

        assert "detail" in data

    @pytest.mark.parametrize(
        "invalid_case",
        [
            "missing_required_field",
            "negative_amount",
            "invalid_risk_range",
            "negative_count",
        ],
    )
    def test_predict_all_validation_errors(
        self, client, sample_invalid_transactions, invalid_case
    ):
        """
        Test: All validation error cases

        Parametrized test covering all validation error scenarios.
        This consolidates multiple test cases into one test function.
        FastAPI returns 422 for request validation errors.

        Given: Invalid transaction data from fixture
        When: POST request is made to /predict endpoint
        Then:
            - Response status is 422 Unprocessable Entity
            - Error message is present in response

        Args:
            invalid_case: Key for the invalid data case to test
        """
        invalid_data = sample_invalid_transactions[invalid_case]
        response = client.post("/predict", json=invalid_data)

        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
        data = response.json()

        assert "detail" in data

    def test_predict_response_structure(
        self, client, sample_valid_transaction, mock_model
    ):
        """
        Test: Response structure matches expected schema

        Verifies that the response contains all required fields
        and that field types are correct. This ensures API contract compliance.

        Given: Valid transaction data
        When: POST request is made to /predict endpoint
        Then:
            - All required fields are present
            - Field types match specification
            - Probability is in valid range [0, 1]
        """
        mock_model.predict_proba.return_value = np.array([[0.92, 0.08]])

        response = client.post("/predict", json=sample_valid_transaction)

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        # Check required fields
        required_fields = [
            "is_fraud",
            "fraud_probability",
            "timestamp",
            "model_version",
        ]
        for field in required_fields:
            assert field in data, f"Missing field: {field}"

        # Check field types
        assert isinstance(data["is_fraud"], bool)
        assert isinstance(data["fraud_probability"], (int, float))
        assert isinstance(data["model_version"], str)

        # Check probability range
        assert 0 <= data["fraud_probability"] <= 1

    def test_predict_multiple_requests(
        self, client, sample_valid_transaction, mock_model
    ):
        """
        Test: Multiple consecutive predictions

        Verifies that the endpoint can handle multiple requests correctly.
        This tests for any state pollution issues.

        Given: Valid transaction data
        When: Multiple POST requests are made to /predict endpoint
        Then:
            - Each request returns correct response
            - Responses are independent (no state pollution)
        """
        responses = []

        for i in range(3):
            mock_model.predict_proba.return_value = np.array(
                [[0.9 - i * 0.1, 0.1 + i * 0.1]]
            )
            response = client.post("/predict", json=sample_valid_transaction)
            responses.append(response.json())

        # Verify each response
        assert responses[0]["fraud_probability"] == 0.1
        assert responses[1]["fraud_probability"] == 0.2
        assert responses[2]["fraud_probability"] == 0.3

        # All should be non-fraud
        assert all(not r["is_fraud"] for r in responses)

    def test_predict_minimal_valid_data(self, client, mock_model):
        """
        Test: Prediction with minimal valid data (only required fields)

        Verifies that the endpoint accepts requests with only required fields.
        This ensures flexibility for clients with minimal data.

        Given: Minimal data with only required fields
        When: POST request is made to /predict endpoint
        Then:
            - Response status is 200 OK
            - Prediction is successful
        """
        minimal_data = {"TX_AMOUNT": 100.0, "IS_WEEKEND": False, "IS_NIGHT": False}

        mock_model.predict_proba.return_value = np.array([[0.98, 0.02]])

        response = client.post("/predict", json=minimal_data)

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        assert "is_fraud" in data
        assert "fraud_probability" in data
