from feature_store.entities import customer, terminal, transaction


def test_transaction_entity_join_keys():
    assert transaction.join_keys == ["transaction_id"]


def test_transaction_entity_name():
    assert transaction.name == "transaction"


def test_customer_entity_join_keys():
    assert customer.join_keys == ["customer_id"]


def test_customer_entity_name():
    assert customer.name == "customer"


def test_terminal_entity_join_keys():
    assert terminal.join_keys == ["terminal_id"]


def test_terminal_entity_name():
    assert terminal.name == "terminal"
