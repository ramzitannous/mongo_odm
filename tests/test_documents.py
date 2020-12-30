from unittest.mock import MagicMock

from motor_odm import configure
from motor_odm.documents import MongoDocument

DB_NAME = "test_db"
COLLECTION_NAME = "test_document"
MOCKED_DB = MagicMock(name="db")
MOCKED_COLLECTION = MagicMock(name="collection")
MOCKED_DB.__getitem__ = lambda _, v: MOCKED_COLLECTION


def _setup_tests():
    mock_client = MagicMock()
    mock_client.__getitem__ = lambda _, v: MOCKED_DB
    configure(mock_client, DB_NAME)


def test_document_without_meta_class():
    _setup_tests()

    class TestDocument(MongoDocument):
        age: int
        name: str

    assert TestDocument.collection_name == COLLECTION_NAME
    assert TestDocument.db_name == DB_NAME


def test_document_with_meta_class():
    _setup_tests()

    class TestDocument(MongoDocument):
        age: int
        name: str

        class Meta:
            collection_name = "col"
            db_name = "db"

    assert TestDocument.collection_name == "col"
    assert TestDocument.db_name == "db"


def test_document_with_meta_class_get_collection_and_db():
    _setup_tests()

    class TestDocument(MongoDocument):
        age: int
        name: str

    assert TestDocument.collection_name == COLLECTION_NAME
    assert TestDocument.db_name == DB_NAME
    assert TestDocument.db is MOCKED_DB
    assert TestDocument.collection is MOCKED_COLLECTION
