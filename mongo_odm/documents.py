from typing import (
    Generic,
    Tuple,
    Type,
    no_type_check,
    TYPE_CHECKING,
    Optional,
    List,
    TypeVar,
    Union,
    Callable,
    Any,
    Dict,
)

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorCollection, AsyncIOMotorDatabase
from pydantic.fields import ModelField
from pydantic.schema import default_ref_template

from mongo_odm.config import get_motor_client, get_db_name
from mongo_odm.fields import PrimaryID
from mongo_odm.registry import register
from mongo_odm.utils import to_snake_case, validate_collection_name
from pydantic import BaseModel, Field
from pydantic.main import ModelMetaclass
from pymongo.collection import IndexModel
from mongo_odm.exceptions import DocumentDoestNotExists
from mongo_odm.managers import MongoBaseManager, MongoQueryManager

if TYPE_CHECKING:  # pragma: no cover
    from pydantic.typing import AbstractSetIntStr, MappingIntStrAny, DictStrAny

T = TypeVar("T", bound="MongoDocument")


class MongoDocumentBaseMetaData(ModelMetaclass):
    """Document MetaClass that configures common behaviour for MongoDocument"""

    if TYPE_CHECKING:  # pragma: no cover
        _collection_name: str
        _db_name: str

    @property
    def collection_name(self) -> str:
        """collection name"""
        return self._collection_name

    @property
    def db_name(self) -> str:
        """db name"""
        return self._db_name

    @property
    def db(self) -> AsyncIOMotorDatabase:
        """returns a reference of the used database"""
        return get_motor_client()[self._db_name]

    @property
    def collection(self) -> AsyncIOMotorCollection:
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

    @staticmethod
    def _manager_field_names(cls: Type["MongoDocument"]) -> List[str]:
        manager_fields = []
        for field_name, field in cls.__fields__.items():
            if issubclass(field.type_, MongoBaseManager):
                manager_fields.append(field_name)
        return manager_fields

    @classmethod
    def _fields_without_managers(
        mcs,
        mongo_document: Type["MongoDocument"],
    ) -> Dict[str, ModelField]:
        manager_fields_names = mcs._manager_field_names(mongo_document)
        all_fields = mongo_document.__fields__.copy()
        for manager_field_name in manager_fields_names:
            del all_fields[manager_field_name]
        return all_fields

    @no_type_check
    def __new__(mcs, name: str, bases: Tuple[type], attr: dict):
        if name == "MongoDocument":
            return super().__new__(mcs, name, bases, attr)

        meta_cls = attr.pop("Meta", None)
        collection_name, db_name = MongoDocumentBaseMetaData._get_config(meta_cls, name)
        attr["_collection_name"] = collection_name
        attr["_db_name"] = db_name
        default_manager = MongoQueryManager()
        attr["_objects"] = default_manager
        created_class = super().__new__(mcs, name, bases, attr)

        # create manager instance
        default_manager.add_to_class(created_class)
        created_class.objects = default_manager

        # set some magic methods on document class
        setattr(
            created_class,
            "__fields_without_managers__",
            mcs._fields_without_managers(created_class),
        )
        setattr(
            created_class,
            "__manager_field_names__",
            mcs._manager_field_names(created_class),
        )

        register(created_class)
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


class MongoDocument(Generic[T], BaseModel, metaclass=MongoDocumentBaseMetaData):
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

    # added by metaclass
    if TYPE_CHECKING:  # pragma: no cover
        collection: AsyncIOMotorCollection
        collection_name: str
        db_name: str
        db: AsyncIOMotorDatabase
        objects: MongoQueryManager[T]
        _objects: MongoQueryManager[T]
        __fields_without_managers__: Dict[str, ModelField]
        __manager_field_names__: List[str]

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str, PrimaryID: str}
        validate_assignment = True
        allow_population_by_field_name = True

    class Meta:
        collection_name = None
        db_name = None

    id: Optional[PrimaryID] = Field(alias="_id")

    @property
    def _db(self) -> AsyncIOMotorDatabase:
        return self.__class__.db

    @classmethod
    def schema(
        cls, by_alias: bool = False, ref_template: str = default_ref_template
    ) -> "DictStrAny":
        """schema of the document"""
        return super().schema(by_alias, ref_template)

    @classmethod
    def schema_json(
        cls,
        *,
        by_alias: bool = False,
        ref_template: str = default_ref_template,
        **dumps_kwargs: Any,
    ) -> str:
        return super().schema_json(
            by_alias=by_alias, ref_template=ref_template, **dumps_kwargs
        )

    @classmethod
    async def drop_collection(cls) -> None:
        """drop the current collection"""
        await cls.collection.drop()

    @classmethod
    async def create_indexes(
        cls, indexes: List[IndexModel], **kwargs: Dict[str, Any]
    ) -> None:
        """create indexes for the current collection, using pymongo.collection"""
        await cls.collection.create_indexes(indexes, **kwargs)  # pragma: no cover

    @classmethod
    async def drop_indexes(cls, **kwargs: Dict[str, Any]) -> None:
        """drop all indexes of the currenct collection"""
        await cls.collection.drop_indexes(**kwargs)  # pragma: no cover

    @property
    def _collection(self) -> AsyncIOMotorCollection:
        return self.__class__.collection

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
        """dict representation of the document"""

        if exclude is not None:
            new_excluded_fields = {*self.__manager_field_names__, *exclude}
        else:
            new_excluded_fields = {*self.__manager_field_names__}
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
        """json representation of the document"""
        if exclude is not None:
            new_excluded_fields = {*self.__manager_field_names__, *exclude}
        else:
            new_excluded_fields = {*self.__manager_field_names__}
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
                f"cant reload document with id {self.id}," f" because it doesn't exists"
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

    @classmethod
    def construct(cls, _fields_set: set = None, **values: Any) -> T:
        """construct document recursively without validation"""
        # https://github.com/samuelcolvin/pydantic/issues/1168

        model = cls.__new__(cls)
        fields_values = {}

        for name, field in cls.__fields_without_managers__.items():
            key = field.alias
            if key in values:
                try:
                    fields_values[name] = field.outer_type_.construct(**values[key])
                except AttributeError:
                    if values[key] is None and not field.required:
                        fields_values[name] = field.get_default()
                    else:
                        fields_values[name] = values[key]
            else:
                fields_values[name] = field.get_default()

        object.__setattr__(model, "__dict__", fields_values)
        if _fields_set is None:
            _fields_set = set(values.keys())
        object.__setattr__(model, "__fields_set__", _fields_set)
        model._init_private_attributes()
        return model
