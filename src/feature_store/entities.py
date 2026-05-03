from feast import Entity, ValueType

# value_type is required in Feast 0.47; join_key (singular string) is the stored attribute.
transaction = Entity(name="transaction", join_keys=["transaction_id"], value_type=ValueType.INT64)
customer = Entity(name="customer", join_keys=["customer_id"], value_type=ValueType.STRING)
terminal = Entity(name="terminal", join_keys=["terminal_id"], value_type=ValueType.STRING)
