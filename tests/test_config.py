from unittest.mock import MagicMock

import pytest
from motor_odm import (
    ImproperlyConfigured,
    configure,
    disconnect,
    get_db_name,
    get_motor_client,
)


def test_configure_function():
    client_mock = MagicMock("test db")
    mocked_database = MagicMock("mocked database")
    client_mock.__getitem__.side_effect = lambda _: mocked_database
    client_mock.close = lambda: None
    db_name = "test"
    configure(client_mock, db_name)
    assert get_motor_client() is client_mock
    disconnect()


def test_get_configured_db_name():
    db_name = "test"
    configure(MagicMock(), db_name)
    assert get_db_name() == db_name
    disconnect()


def test_get_configured_db_name_raises_error():
    with pytest.raises(ImproperlyConfigured):
        get_db_name()


def test_raises_exception_on_wrong_configuration():
    with pytest.raises(ImproperlyConfigured):
        get_motor_client()


def test_disconnects_correctly_if_configured():
    client_mock = MagicMock()
    configure(client_mock, "test")
    disconnect_mock = MagicMock()
    client_mock.close = disconnect_mock
    disconnect()
    disconnect_mock.assert_called_once()
