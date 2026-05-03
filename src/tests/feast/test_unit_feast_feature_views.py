from datetime import timedelta

from feature_store.feature_views.customer_features import customer_features_view
from feature_store.feature_views.fraud_ml_features import fraud_ml_features_view
from feature_store.feature_views.terminal_features import terminal_features_view

# --- fraud_ml_features_view ---

def test_fraud_ml_features_view_name():
    assert fraud_ml_features_view.name == "fraud_ml_features_view"


def test_fraud_ml_features_view_ttl():
    assert fraud_ml_features_view.ttl == timedelta(days=365)


def test_fraud_ml_features_view_feature_names():
    expected = {
        "TX_AMOUNT",
        "IS_WEEKEND",
        "IS_NIGHT",
        "CUSTOMER_AVG_AMOUNT_WINDOW_1D",
        "CUSTOMER_AVG_AMOUNT_WINDOW_7D",
        "CUSTOMER_AVG_AMOUNT_WINDOW_30D",
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D",
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D",
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D",
        "TERMINAL_RISK_1DAY_WINDOW",
        "TERMINAL_RISK_7DAY_WINDOW",
        "TERMINAL_RISK_30DAY_WINDOW",
        "TERMINAL_NB_TX_1DAY_WINDOW",
        "TERMINAL_NB_TX_7DAY_WINDOW",
        "TERMINAL_NB_TX_30DAY_WINDOW",
        "TX_FRAUD",
    }
    assert {f.name for f in fraud_ml_features_view.schema} == expected


def test_fraud_ml_features_view_is_offline():
    assert fraud_ml_features_view.online is False


# --- customer_features_view ---

def test_customer_features_view_name():
    assert customer_features_view.name == "customer_features_view"


def test_customer_features_view_ttl():
    assert customer_features_view.ttl == timedelta(days=2)


def test_customer_features_view_feature_names():
    expected = {
        "CUSTOMER_AVG_AMOUNT_WINDOW_1D",
        "CUSTOMER_AVG_AMOUNT_WINDOW_7D",
        "CUSTOMER_AVG_AMOUNT_WINDOW_30D",
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D",
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D",
        "CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D",
    }
    assert {f.name for f in customer_features_view.schema} == expected


def test_customer_features_view_is_online():
    assert customer_features_view.online is True


# --- terminal_features_view ---

def test_terminal_features_view_name():
    assert terminal_features_view.name == "terminal_features_view"


def test_terminal_features_view_ttl():
    assert terminal_features_view.ttl == timedelta(days=2)


def test_terminal_features_view_feature_names():
    expected = {
        "TERMINAL_RISK_1DAY_WINDOW",
        "TERMINAL_RISK_7DAY_WINDOW",
        "TERMINAL_RISK_30DAY_WINDOW",
        "TERMINAL_NB_TX_1DAY_WINDOW",
        "TERMINAL_NB_TX_7DAY_WINDOW",
        "TERMINAL_NB_TX_30DAY_WINDOW",
    }
    assert {f.name for f in terminal_features_view.schema} == expected


def test_terminal_features_view_is_online():
    assert terminal_features_view.online is True
