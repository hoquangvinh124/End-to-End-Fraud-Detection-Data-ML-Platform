from feast import Entity

from feature_store.entities import customer, terminal, transaction


def test_transaction_is_feast_entity():
    assert isinstance(transaction, Entity)


def test_transaction_entity_join_key():
    assert transaction.join_key == "transaction_id"


def test_transaction_entity_name():
    assert transaction.name == "transaction"


def test_customer_entity_join_key():
    assert customer.join_key == "customer_id"


def test_customer_entity_name():
    assert customer.name == "customer"


def test_terminal_entity_join_key():
    assert terminal.join_key == "terminal_id"


def test_terminal_entity_name():
    assert terminal.name == "terminal"
