from datetime import timedelta

from feast import FeatureView, Field, FileSource
from feast.types import Float64, Int64

from feature_store.entities import terminal

_source = FileSource(
    path="s3://gold/terminal_features/",
    timestamp_field="feature_date",
)

terminal_features_view = FeatureView(
    name="terminal_features_view",
    entities=[terminal],
    ttl=timedelta(days=2),
    schema=[
        Field(name="TERMINAL_RISK_1DAY_WINDOW", dtype=Float64),
        Field(name="TERMINAL_RISK_7DAY_WINDOW", dtype=Float64),
        Field(name="TERMINAL_RISK_30DAY_WINDOW", dtype=Float64),
        Field(name="TERMINAL_NB_TX_1DAY_WINDOW", dtype=Int64),
        Field(name="TERMINAL_NB_TX_7DAY_WINDOW", dtype=Int64),
        Field(name="TERMINAL_NB_TX_30DAY_WINDOW", dtype=Int64),
    ],
    source=_source,
    online=True,
)
