from typing import Tuple, Type, no_type_check

from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from motor_odm import get_db_name, get_motor_client
from motor_odm.utils import to_snake_case, validate_collection_name
from pydantic import BaseModel
from pydantic.main import ModelMetaclass


class MongoDocumentBaseMetaData(ModelMetaclass):
    """
    Document MetaClass that configures common behaviour for MongoDocument
    """

    _collection_name: str
    _db_name: str

    @property
    def collection_name(self) -> str:
        return MongoDocumentBaseMetaData._collection_name

    @property
    def db_name(self) -> str:
        return MongoDocumentBaseMetaData._db_name

    """
        returns a reference of the used database
    """

    @property
    def db(self) -> AsyncIOMotorDatabase:
        """
        returns a reference of the used database
        """
        return get_motor_client()[self.db_name]

    @property
    def collection(self) -> AsyncIOMotorCollection:
        """
        returns a reference of the used collection in the db
        """
        return get_motor_client()[self.db_name][self.collection_name]

    @staticmethod
    def _get_config(
        meta_cls: Type["MongoDocument.Meta"], document_class_name: str
    ) -> Tuple[str, str]:
        collection_name = to_snake_case(document_class_name)
        db_name = get_db_name()

        Meta = meta_cls
        if Meta is not None:
            collection_name = getattr(Meta, "collection_name", collection_name)
            db_name = getattr(Meta, "db_name", db_name)

            if collection_name is not None:
                validate_collection_name(collection_name, document_class_name)

        return collection_name, db_name

    @no_type_check
    def __new__(mcs, name: str, bases: Tuple[type], attr: dict):
        if name == "MongoDocument":
            return super().__new__(mcs, name, bases, attr)

        meta_cls = attr.pop("Meta", None)
        collection_name, db_name = MongoDocumentBaseMetaData._get_config(meta_cls, name)
        mcs._collection_name = collection_name
        mcs._db_name = db_name
        return super().__new__(mcs, name, bases, attr)


class MongoDocumentMeta:
    """
    Document Meta that specify or override document default behaviour
    """

    """
    Default collection name is the plural snake_case of the document class
     eg. ProductItem -> product_items
     this field overrides the default name to anything specified
    """
    collection_name: str

    """
    overrides db_name from configuration
    """
    db_name: str


class MongoDocument(BaseModel, metaclass=MongoDocumentBaseMetaData):
    """
    Base Mongodb document that other documents should inherit to use common functionality,
    represents a document in mongodb collection
    primary id of type fields.PrimaryID is automatically generated

    eg. without Meta class

    class PersonDocument(MongoDocument):
        age: int
        name: str

    with Meta class to override configuration

    class PersonDocument(MongoDocument):
        age: int
        name: str

        class Meta:
            collection_name = 'person'
            db_name = 'people_db'
    """

    class Config:
        orm_mode = True

    class Meta:
        collection_name = None
        db_name = None
