import pytest
from datetime import datetime
from api.models import TransactionRequest, PredictionResponse, HealthResponse, ErrorResponse
from pydantic import ValidationError

class TestTransactionRequest:
    """Test suite for TransactionRequest model"""

    def test_valid_transaction(self, sample_valid_transaction):
        """
        Test: Create TransactionRequest with valid data

        Verifies that valid transaction data passes validation.
        This is a baseline test to ensure basic validation works.

        Given: Valid transaction dictionary with all required fields
        When: TransactionRequest instance is created
        Then: Instance is created successfully with correct values
        """
        request = TransactionRequest(**sample_valid_transaction)

        assert request.TX_AMOUNT == 150.50
        assert request.CUSTOMER_AVG_AMOUNT_WINDOW_1D == 120.0
        assert request.CUSTOMER_AVG_AMOUNT_WINDOW_7D == 110.5
        assert request.CUSTOMER_AVG_AMOUNT_WINDOW_30D == 105.3
        assert request.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D == 2.0
        assert request.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D == 8.0
        assert request.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D == 25.0
        assert request.TERMINAL_RISK_1DAY_WINDOW == 0.02
        assert request.TERMINAL_RISK_7DAY_WINDOW == 0.015
        assert request.TERMINAL_RISK_30DAY_WINDOW == 0.01
        assert request.TERMINAL_NB_TX_1DAY_WINDOW == 50.0
        assert request.TERMINAL_NB_TX_7DAY_WINDOW == 300.0
        assert request.TERMINAL_NB_TX_30DAY_WINDOW == 1200.0
        assert request.IS_WEEKEND is False
        assert request.IS_NIGHT is False

    @pytest.mark.parametrize("missing_field", ["TX_AMOUNT", "IS_WEEKEND", "IS_NIGHT"])
    def test_missing_required_field(self, sample_valid_transaction, missing_field):
        """
        Test: ValidationError when required field is missing

        Verifies that required fields are enforced by validation.
        Uses parameterized testing to check all required fields.

        Given: Valid data with one required field removed
        When: TransactionRequest instance is created
        Then: ValidationError is raised with appropriate message

        Args:
            missing_field: The field to remove from the data
        """
        invalid_data = sample_valid_transaction.copy()
        invalid_data.pop(missing_field)

        with pytest.raises(ValidationError) as exc_info:
            TransactionRequest(**invalid_data)

        assert missing_field in str(exc_info.value)

    def test_negative_amount(self, sample_valid_transaction):
        """
        Test: ValidationError for negative transaction amount

        Verifies that TX_AMOUNT must be positive (gt=0 constraint).
        Edge case: negative numbers should be rejected.

        Given: Valid data with negative TX_AMOUNT
        When: TransactionRequest instance is created
        Then: ValidationError is raised
        """
        invalid_data = sample_valid_transaction.copy()
        invalid_data["TX_AMOUNT"] = -100.50

        with pytest.raises(ValidationError) as exc_info:
            TransactionRequest(**invalid_data)

        assert "greater than 0" in str(exc_info.value)

    @pytest.mark.parametrize("risk_field", [
        "TERMINAL_RISK_1DAY_WINDOW",
        "TERMINAL_RISK_7DAY_WINDOW",
        "TERMINAL_RISK_30DAY_WINDOW"
    ])
    def test_invalid_risk_range(self, sample_valid_transaction, risk_field):
        """
        Test: ValidationError when terminal risk exceeds valid range

        Verifies that terminal risk values must be in [0, 1] range.
        Edge cases: values > 1 should be rejected.

        Given: Valid data with risk field set to 1.5
        When: TransactionRequest instance is created
        Then: ValidationError is raised

        Args:
            risk_field: The terminal risk field to test
        """
        invalid_data = sample_valid_transaction.copy()
        invalid_data[risk_field] = 1.5

        with pytest.raises(ValidationError) as exc_info:
            TransactionRequest(**invalid_data)

        assert "less than or equal to 1" in str(exc_info.value)

    @pytest.mark.parametrize("count_field", [
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D",
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D",
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D",
        "TERMINAL_NB_TX_1DAY_WINDOW",
        "TERMINAL_NB_TX_7DAY_WINDOW",
        "TERMINAL_NB_TX_30DAY_WINDOW"
    ])
    def test_negative_count(self, sample_valid_transaction, count_field):
        """
        Test: ValidationError when count values are negative

        Verifies that count fields must be non-negative (ge=0 constraint).
        Edge case: negative numbers should be rejected.

        Given: Valid data with count field set to negative value
        When: TransactionRequest instance is created
        Then: ValidationError is raised

        Args:
            count_field: The count field to test
        """
        invalid_data = sample_valid_transaction.copy()
        invalid_data[count_field] = -5.0

        with pytest.raises(ValidationError) as exc_info:
            TransactionRequest(**invalid_data)

        assert "greater than or equal to 0" in str(exc_info.value)

    def test_default_values(self):
        """
        Test: Default values are applied correctly

        Verifies that optional fields use their default values when not provided.
        This ensures the API accepts minimal valid requests.

        Given: Minimal data with only required fields
        When: TransactionRequest instance is created
        Then: Default values are set for optional fields
        """
        minimal_data = {
            "TX_AMOUNT": 100.0,
            "IS_WEEKEND": False,
            "IS_NIGHT": False
        }

        request = TransactionRequest(**minimal_data)

        assert request.CUSTOMER_AVG_AMOUNT_WINDOW_1D == 0.0
        assert request.CUSTOMER_AVG_AMOUNT_WINDOW_7D == 0.0
        assert request.CUSTOMER_AVG_AMOUNT_WINDOW_30D == 0.0
        assert request.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D == 0.0
        assert request.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D == 0.0
        assert request.CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D == 0.0
        assert request.TERMINAL_RISK_1DAY_WINDOW == 0.0
        assert request.TERMINAL_RISK_7DAY_WINDOW == 0.0
        assert request.TERMINAL_RISK_30DAY_WINDOW == 0.0
        assert request.TERMINAL_NB_TX_1DAY_WINDOW == 0.0
        assert request.TERMINAL_NB_TX_7DAY_WINDOW == 0.0
        assert request.TERMINAL_NB_TX_30DAY_WINDOW == 0.0


class TestPredictionResponse:
    """Test suite for PredictionResponse model"""

    def test_valid_response(self):
        """
        Test: Create PredictionResponse with valid data

        Verifies that prediction response data is validated correctly.
        Baseline test for response model validation.

        Given: Valid prediction data
        When: PredictionResponse instance is created
        Then: Instance is created successfully with correct values
        """
        response = PredictionResponse(
            is_fraud=False,
            fraud_probability=0.05,
            model_version="test_model_v1"
        )

        assert response.is_fraud is False
        assert response.fraud_probability == 0.05
        assert response.model_version == "test_model_v1"
        assert isinstance(response.timestamp, datetime)

    @pytest.mark.parametrize("invalid_prob", [-0.1, 1.5, 2.0])
    def test_invalid_probability_range(self, invalid_prob):
        """
        Test: ValidationError for probability outside [0, 1] range

        Verifies that fraud_probability must be in valid range [0, 1].
        Edge cases: negative values and values > 1 should be rejected.

        Given: Valid data with invalid probability value
        When: PredictionResponse instance is created
        Then: ValidationError is raised

        Args:
            invalid_prob: The invalid probability value to test
        """
        with pytest.raises(ValidationError) as exc_info:
            PredictionResponse(
                is_fraud=True,
                fraud_probability=invalid_prob,
                model_version="test_model"
            )

        assert "greater than or equal to 0" in str(exc_info.value) or \
               "less than or equal to 1" in str(exc_info.value)

    @pytest.mark.parametrize("is_fraud, prob", [
        (True, 0.75),
        (False, 0.25)
    ])
    def test_fraud_label_probability_consistency(self, is_fraud, prob):
        """
        Test: Fraud label and probability relationship

        Verifies that the relationship between is_fraud and fraud_probability
        makes sense. In production, is_fraud should be True when prob >= 0.5.
        This test just validates the types, not the business logic.

        Given: Valid prediction data
        When: PredictionResponse instance is created
        Then: Both fields are correctly set
        """
        response = PredictionResponse(
            is_fraud=is_fraud,
            fraud_probability=prob,
            model_version="test_model"
        )

        assert response.is_fraud == is_fraud
        assert response.fraud_probability == prob


class TestHealthResponse:
    """Test suite for HealthResponse model"""

    def test_valid_health_response(self):
        """
        Test: Create HealthResponse with valid data

        Verifies that health check response is validated correctly.

        Given: Valid health check data
        When: HealthResponse instance is created
        Then: Instance is created successfully
        """
        response = HealthResponse(
            status="healthy",
            model_loaded=True,
            model_version="model_v1"
        )

        assert response.status == "healthy"
        assert response.model_loaded is True
        assert response.model_version == "model_v1"
        assert isinstance(response.timestamp, datetime)

    def test_unhealthy_status(self):
        """
        Test: HealthResponse with unhealthy status

        Verifies that health check can report unhealthy status.

        Given: Unhealthy health check data
        When: HealthResponse instance is created
        Then: Status is correctly set to unhealthy
        """
        response = HealthResponse(
            status="unhealthy",
            model_loaded=False,
            model_version="not_loaded"
        )

        assert response.status == "unhealthy"
        assert response.model_loaded is False
        assert response.model_version == "not_loaded"


class TestErrorResponse:
    """Test suite for ErrorResponse model"""

    def test_valid_error_response(self):
        """
        Test: Create ErrorResponse with valid data

        Verifies that error response is validated correctly.

        Given: Valid error response data
        When: ErrorResponse instance is created
        Then: Instance is created successfully
        """
        response = ErrorResponse(
            error="Validation error",
            detail="Invalid input data"
        )

        assert response.error == "Validation error"
        assert response.detail == "Invalid input data"
        assert isinstance(response.timestamp, datetime)

    def test_error_response_without_detail(self):
        """
        Test: ErrorResponse with only required error message

        Verifies that detail field is optional.

        Given: Error response with only error field
        When: ErrorResponse instance is created
        Then: Detail defaults to None
        """
        response = ErrorResponse(
            error="Server error"
        )

        assert response.error == "Server error"
        assert response.detail is None
        assert isinstance(response.timestamp, datetime)
