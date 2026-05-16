from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64, Int64

from feature_store.entities import customer

_source = FileSource(
    path="s3://gold/customer_features/",
    timestamp_field="feature_date",
)

customer_features_view = FeatureView(
    name="customer_features_view",
    entities=[customer],
    ttl=timedelta(days=2),
    schema=[
        Field(name="CUSTOMER_AVG_AMOUNT_WINDOW_1D", dtype=Float64),
        Field(name="CUSTOMER_AVG_AMOUNT_WINDOW_7D", dtype=Float64),
        Field(name="CUSTOMER_AVG_AMOUNT_WINDOW_30D", dtype=Float64),
        Field(name="CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_1D", dtype=Int64),
        Field(name="CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_7D", dtype=Int64),
        Field(name="CUSTOMER_NUMBER_OF_TRANSACTIONS_WINDOW_30D", dtype=Int64),
    ],
    source=_source,
    online=True,
)
