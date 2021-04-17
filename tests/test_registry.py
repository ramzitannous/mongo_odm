import pytest

from mongo_odm.exceptions import AlreadyRegistered
from mongo_odm.registry import DOCUMENTS_REGISTRY, clear_registry, register, unregister


def _setup():
    from mongo_odm.documents import MongoDocument

    class TestRegisterADocument(MongoDocument):
        pass

    return TestRegisterADocument


def test_register_a_document():
    TestRegisterADocument = _setup()
    assert "TestRegisterADocument" in DOCUMENTS_REGISTRY
    unregister(TestRegisterADocument)


def test_unregister_a_document():
    TestRegisterADocument = _setup()
    unregister(TestRegisterADocument)
    assert "TestRegisterADocument" not in DOCUMENTS_REGISTRY


def test_clear_registry():
    clear_registry()
    assert not DOCUMENTS_REGISTRY.keys()


def test_register_a_document_twice():
    with pytest.raises(AlreadyRegistered):
        TestRegisterADocument = _setup()
        register(TestRegisterADocument)


def test_try_register_wrong_type():
    class T:
        pass

    with pytest.raises(TypeError):
        register(T)
