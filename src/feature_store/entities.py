from feast import Entity as FeastEntity


class Entity:
    """Wrapper around Feast Entity that exposes join_keys as a list for compatibility."""

    def __init__(self, name: str, join_keys: list[str]):
        self.name = name
        self.join_keys = join_keys
        self._feast_entity = FeastEntity(name=name, join_keys=join_keys)

    def __getattr__(self, attr):
        # Delegate other attributes to the Feast Entity
        return getattr(self._feast_entity, attr)


# value_type is intentionally omitted — deprecated in Feast >=0.40; types are inferred from Field schema.
transaction = Entity(name="transaction", join_keys=["transaction_id"])
customer = Entity(name="customer", join_keys=["customer_id"])
terminal = Entity(name="terminal", join_keys=["terminal_id"])
