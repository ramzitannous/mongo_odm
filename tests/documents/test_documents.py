from typing import List, Optional
from unittest.mock import MagicMock

import pytest
from bson import ObjectId
from pydantic import Field, ValidationError

from tests.document import PersonDocument

from motor_odm.config import configure, disconnect
from motor_odm.documents import MongoDocument
from motor_odm.managers import MongoBaseManager

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


def test_document_with_correct_db_used():
    _setup_tests()

    class TestDocument4(MongoDocument):
        name: str

    t = TestDocument4(name="test")
    assert t._db is MOCKED_DB
    disconnect()


def test_load_document_from_db():
    db_document = {"_id": ObjectId(ID)}
    document = MongoDocument(**db_document)
    assert isinstance(document.id, ObjectId)


def test_document_primary_key_serialization():
    p = PersonDocument(age=10, name="ram1")
    p.id = ObjectId(ID)
    assert p.json()


def test_document_type():
    p = PersonDocument(age=10, name="ram")
    assert type(p) == PersonDocument
    assert isinstance(p, PersonDocument)


def test_document_validate_field():
    p = PersonDocument(age=10, name="test")
    with pytest.raises(ValidationError):
        p.age = "ram"


def test_document_schema():
    schema = PersonDocument.schema()
    assert schema["properties"]["id"] is not None
    assert schema["properties"]["id"]["title"] == "Id"


def test_document_schema_json():
    schema = PersonDocument.schema_json()
    assert '"id": {"title": "Id"' in schema


def test_correct_returned_fields_without_managers():
    class TestWithManagers(MongoDocument):
        custom_manager = MongoBaseManager()

        name: str
        age: int
        salary: float

    assert list(TestWithManagers.__fields_without_managers__.keys()) == [
        "id",
        "name",
        "age",
        "salary",
    ]


def test_document_construct_with_default_values():
    class TestDocument(MongoDocument):
        name: str
        age: int = 10
        salary: Optional[float] = None
        list_field: List[str] = Field(default_factory=list)

    document_dict = {"name": "Ramzi"}
    constructed_document = TestDocument.construct(**document_dict)
    assert constructed_document.name == "Ramzi"
    assert constructed_document.age == 10
    assert constructed_document.salary is None
    assert constructed_document.list_field == []
    assert constructed_document.id is None


def test_nested_document_construct_with_default_values():
    class ADocument(MongoDocument):
        name: str
        age: int = 10
        salary: Optional[float]
        list_field: List[str] = Field(default_factory=list)

    class BDocument(MongoDocument):
        a: ADocument
        f: str

    document_dict = {"a": {"name": "Ramzi", "salary": None}}
    constructed_document = BDocument.construct(**document_dict)
    assert constructed_document.a.name == "Ramzi"
    assert constructed_document.a.age == 10
    assert constructed_document.a.salary is None
    assert constructed_document.a.list_field == []
    assert constructed_document.id is None
    assert constructed_document.f is None
