from motor_odm.documents import MongoDocument
from motor_odm.managers import MongoBaseManager, MongoQueryManager


def test_document_has_a_default_manager():
    class T2(MongoDocument):
        pass

    assert isinstance(T2.objects, MongoQueryManager)


def test_document_instance_has_an_internal_manager():
    class TestWithInternalManager(MongoDocument):
        pass

    t = TestWithInternalManager()
    assert isinstance(t._objects, MongoQueryManager)


def test_document_exclude_managers_when_calling_dict():
    class CustomManager(MongoBaseManager):
        pass

    class T1(MongoDocument):
        custom_manager = CustomManager()
        name: str

    assert isinstance(T1.custom_manager, MongoBaseManager)
    _t = T1(name="test")
    _dict = _t.dict(exclude={"name"})
    assert "custom_manager" not in _dict
    assert "name" not in _dict
    _dict = _t.dict()
    assert "custom_manager" not in _dict


def test_document_exclude_managers_when_calling_json():
    class CustomManager(MongoBaseManager):
        pass

    class T(MongoDocument):
        custom_manager = CustomManager()
        name: str

    _t = T(name="test")
    _json = _t.json()
    assert "custom_manager" not in _json
    _json = _t.json(exclude={"name"})
    assert "custom_manager" not in _json
    assert "name" not in _json
