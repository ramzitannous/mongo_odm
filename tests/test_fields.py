import pytest
from bson import ObjectId
from pydantic import BaseModel

from motor_odm.fields import PrimaryID


def test_primary_key_field_serialization():
    class TestModel(BaseModel):
        id: PrimaryID

    str_id = "5349b4ddd2781d08c09890f3"
    object_id = PrimaryID(str_id)
    test_model = TestModel(id=object_id)
    assert test_model.id == ObjectId(str_id)


def test_invalid_primary_key():
    class TestModel(BaseModel):
        id: PrimaryID

    str_id = "5349b4ddd2781d08c09890"
    object_id = PrimaryID(str_id)
    with pytest.raises(ValueError):
        TestModel(id=object_id)


def test_primary_key_schema():
    schema = {}
    PrimaryID.__modify_schema__(schema)
    assert schema["type"] == "string"
