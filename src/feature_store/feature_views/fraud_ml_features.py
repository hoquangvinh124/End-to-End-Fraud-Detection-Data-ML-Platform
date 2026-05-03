from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Bool, Float64, Int64

from feature_store.entities import transaction

_source = FileSource(
    # s3:// (not s3a://) — Feast uses boto3/s3fs, not Hadoop FileSystem
    path="s3://gold/fraud_detection_ml_features/",
    timestamp_field="event_timestamp",
)

fraud_ml_features_view = FeatureView(
    name="fraud_ml_features_view",
    entities=[transaction],
    ttl=timedelta(days=365),
    schema=[
        Field(name="TX_AMOUNT", dtype=Float64),
        Field(name="IS_WEEKEND", dtype=Bool),
        Field(name="IS_NIGHT", dtype=Bool),
        Field(name="CUSTOMER_AVG_AMOUNT_WINDOW_1D", dtype=Float64),
        Field(name="CUSTOMER_AVG_AMOUNT_WINDOW_7D", dtype=Float64),
        Field(name="CUSTOMER_AVG_AMOUNT_WINDOW_30D", dtype=Float64),
        Field(name="CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D", dtype=Int64),
        Field(name="CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D", dtype=Int64),
        Field(name="CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D", dtype=Int64),
        Field(name="TERMINAL_RISK_1DAY_WINDOW", dtype=Float64),
        Field(name="TERMINAL_RISK_7DAY_WINDOW", dtype=Float64),
        Field(name="TERMINAL_RISK_30DAY_WINDOW", dtype=Float64),
        Field(name="TERMINAL_NB_TX_1DAY_WINDOW", dtype=Int64),
        Field(name="TERMINAL_NB_TX_7DAY_WINDOW", dtype=Int64),
        Field(name="TERMINAL_NB_TX_30DAY_WINDOW", dtype=Int64),
        Field(name="TX_FRAUD", dtype=Int64),
    ],
    source=_source,
    online=False,
)
