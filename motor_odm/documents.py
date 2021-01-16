from typing import (
    Tuple,
    Type,
    no_type_check,
    TYPE_CHECKING,
    Optional,
    List,
    Union,
    Callable,
    Any,
)

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from motor_odm import PrimaryID, get_db_name, get_motor_client
from motor_odm.registry import register
from motor_odm.utils import to_snake_case, validate_collection_name
from pydantic import BaseModel, Field
from pydantic.main import ModelMetaclass
from pymongo.collection import Collection
from motor_odm.exceptions import DocumentDoestNotExists
from motor_odm.managers import MongoBaseManager, MongoDefaultManager

if TYPE_CHECKING:
    from pydantic.typing import AbstractSetIntStr, MappingIntStrAny, DictStrAny


class MongoDocumentBaseMetaData(ModelMetaclass):
    """Document MetaClass that configures common behaviour for MongoDocument"""

    if TYPE_CHECKING:
        _collection_name: str
        _db_name: str

    @property
    def collection_name(self) -> str:
        return self._collection_name

    @property
    def db_name(self) -> str:
        return self._db_name

    @property
    def db(self) -> AsyncIOMotorDatabase:
        """returns a reference of the used database"""
        return get_motor_client()[self._db_name]

    @property
    def collection(self) -> Collection:
        """returns a reference of the used collection in the db"""
        return get_motor_client()[self._db_name][self._collection_name]

    @staticmethod
    def _get_config(
        meta_cls: Optional[Type["MongoDocument.Meta"]], document_class_name: str
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
        attr["_collection_name"] = collection_name
        attr["_db_name"] = db_name
        created_class = super().__new__(mcs, name, bases, attr)
        # add default manager
        created_class.objects = MongoDefaultManager()
        created_class.objects.add_to_class(created_class)
        register(created_class)

        # add class reference to managers
        for attr_name, attr_value in attr.items():
            if isinstance(attr_value, MongoBaseManager):
                attr_value.add_to_class(created_class)
                setattr(created_class, attr_name, attr_value)
        return created_class


class MongoDocumentMeta:
    """Document Meta that specify or override document default behaviour"""

    """Default collection name is the plural snake_case of the document class
     eg. ProductItem -> product_items
     this field overrides the default name to anything specified"""
    collection_name: str

    """overrides db_name from configuration"""
    db_name: str


class MongoDocument(BaseModel, metaclass=MongoDocumentBaseMetaData):
    """Base Mongodb document class that other documents should inherit to use common functionality,
    represents a document in mongodb collection
    primary id of type fields.PrimaryID is automatically generated

    eg. without Meta class:

    class PersonDocument(MongoDocument):
        age: int

        name: str

    with Meta class to override configuration:

    class PersonDocument(MongoDocument):
        age: int

        name: str

        class Meta:
            collection_name = 'person'

            db_name = 'people_db'
    """

    if TYPE_CHECKING:
        collection: Collection
        collection_name: str
        db_name: str
        db: AsyncIOMotorDatabase
        objects: MongoDefaultManager

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str, PrimaryID: str}
        validate_assignment = True

    class Meta:
        collection_name = None
        db_name = None

    id: Optional[PrimaryID] = Field(alias="_id")

    @property
    def _db(self) -> AsyncIOMotorDatabase:
        return self.__class__.db

    @property
    def _collection(self) -> Collection:
        return self.__class__.collection

    def _exclude_managers(self) -> List[str]:
        excluded_fields = []
        for field_name, field in self.__fields__.items():
            if issubclass(field.type_, MongoBaseManager):
                excluded_fields.append(field_name)
        return excluded_fields

    def dict(
        self,
        *,
        include: Union["AbstractSetIntStr", "MappingIntStrAny"] = None,
        exclude: Union["AbstractSetIntStr", "MappingIntStrAny"] = None,
        by_alias: bool = False,
        skip_defaults: bool = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> "DictStrAny":

        excluded_managers = self._exclude_managers()
        if exclude is not None:
            new_excluded_fields = {*excluded_managers, *exclude}
        else:
            new_excluded_fields = {*excluded_managers}
        return super().dict(
            include=include,
            exclude=new_excluded_fields,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
        )

    def json(
        self,
        *,
        include: Union["AbstractSetIntStr", "MappingIntStrAny"] = None,
        exclude: Union["AbstractSetIntStr", "MappingIntStrAny"] = None,
        by_alias: bool = False,
        skip_defaults: bool = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        encoder: Optional[Callable[[Any], Any]] = None,
        **dumps_kwargs: Any,
    ) -> str:
        excluded_managers = self._exclude_managers()
        if exclude is not None:
            new_excluded_fields = {*excluded_managers, *exclude}
        else:
            new_excluded_fields = {*excluded_managers}
        return super().json(
            include=include,
            exclude=new_excluded_fields,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude_none=exclude_none,
            encoder=encoder,
            **dumps_kwargs,
        )

    async def save(self, *excluded_fields: str) -> None:
        """save a single document to collection

        :param excluded_fields: field names to exclude
        :return: None"""

        json_document = self.dict(exclude=set(excluded_fields))
        _id = json_document.pop("id")
        if self.id and ObjectId.is_valid(self.id):  # has id and valid id
            new_document = await self._collection.replace_one(
                {"_id": _id}, json_document, upsert=True
            )
            if new_document.upserted_id is not None:  # updated
                self.id = new_document.upserted_id
        else:
            document = await self._collection.insert_one(json_document)
            self.id = document.inserted_id

    async def reload(self) -> None:
        """reload document from db"""

        if self.id is None:
            raise DocumentDoestNotExists("document is not saved in the db")

        document = await self._collection.find_one({"_id": self.id})
        if document is None:
            raise DocumentDoestNotExists(
                f"can't reload document with id {self.id}, because it doesn't exists"
            )
        for k, v in document.items():
            if k == "_id":
                k = "id"
            setattr(self, k, v)

    async def delete(self) -> None:
        """delete a document from db"""

        result = await self._collection.delete_one({"_id": self.id})
        if not result.deleted_count:
            raise DocumentDoestNotExists(
                f"can't delete document with id {self.id}, because it doesn't exists"
            )
