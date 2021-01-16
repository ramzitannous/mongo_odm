from unittest.mock import MagicMock

import pytest
from bson import ObjectId
from pydantic import ValidationError

from tests.document import PersonDocument

from motor_odm import configure, disconnect
from motor_odm.documents import MongoDocument

DB_NAME = "test_db"
COLLECTION_NAME = "test_document"
MOCKED_DB = MagicMock(name="db")
MOCKED_COLLECTION = MagicMock(name="collection")
MOCKED_DB.__getitem__ = lambda _, v: MOCKED_COLLECTION
ID = "5349b4ddd2781d08c09890f3"


def _setup_tests():
    mock_client = MagicMock()
    mock_client.__getitem__ = lambda _, v: MOCKED_DB
    configure(mock_client, DB_NAME)


def test_document_without_meta_class():
    _setup_tests()

    class TestDocument1(MongoDocument):
        age: int
        name: str

    assert TestDocument1.collection_name == "test_document1"
    assert TestDocument1.db_name == DB_NAME
    disconnect()


def test_document_with_meta_class():
    _setup_tests()

    class TestDocument2(MongoDocument):
        age: int
        name: str

        class Meta:
            collection_name = "col"
            db_name = "db"

    assert TestDocument2.collection_name == "col"
    assert TestDocument2.db_name == "db"
    disconnect()


def test_document_with_meta_class_get_collection_and_db():
    _setup_tests()

    class TestDocument3(MongoDocument):
        age: int
        name: str

    assert TestDocument3.collection_name == "test_document3"
    assert TestDocument3.db_name == DB_NAME
    assert TestDocument3.db is MOCKED_DB
    assert TestDocument3.collection is MOCKED_COLLECTION
    disconnect()


def test_load_document_from_db():
    db_document = {"_id": ObjectId(ID)}
    document = MongoDocument(**db_document)
    assert isinstance(document.id, ObjectId)


def test_document_primary_key_serialization():
    document = MongoDocument()
    document.id = ObjectId(ID)
    assert document.json()


def test_document_type():
    p = PersonDocument(age=10, name="ram")
    assert type(p) == PersonDocument
    assert isinstance(p, PersonDocument)


def test_document_validate_field():
    p = PersonDocument(age=10, name="test")
    with pytest.raises(ValidationError):
        p.age = "ram"
