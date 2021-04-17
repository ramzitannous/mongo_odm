"""registry for document classes, that will help to hook
 document and their managers"""
from typing import Dict


DOCUMENTS_REGISTRY: Dict[str, type] = {}


def register(document_cls: type) -> None:
    """register a document class, for internal use"""

    from mongo_odm.documents import MongoDocumentBaseMetaData

    if not isinstance(document_cls, MongoDocumentBaseMetaData):
        raise TypeError(
            f"class of type {str(document_cls)} can't be registered,"
            f" need to a subclass of  MongoDocument "
        )

    key = document_cls.__name__
    if key in DOCUMENTS_REGISTRY:
        return

    DOCUMENTS_REGISTRY[key] = document_cls


def unregister(document_cls: type) -> None:
    """remove a document class, for internal use"""

    del DOCUMENTS_REGISTRY[document_cls.__name__]


def clear_registry() -> None:
    """clear all, for internal use"""

    DOCUMENTS_REGISTRY.clear()
