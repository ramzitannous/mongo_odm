from __future__ import annotations
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    TYPE_CHECKING,
    Type,
    TypeVar,
    Union,
)

from bson import ObjectId

from motor_odm.cursor import MongoCursor
from motor_odm.exceptions import DocumentDoestNotExists, FieldNotFoundOnDocument

if TYPE_CHECKING:
    from motor_odm.documents import MongoDocument  # noqa

T = TypeVar("T", bound="MongoDocument")


class MongoBaseManager(Generic[T]):
    """Base Manager that all type of managers should inherits
    to create a new manager:

    managers.py

    class CustomManager(MongoBaseManager):
        pass

    documents.py

    class Document(MongoDocument):
        custom_manager = CustomManager()
    """

    if TYPE_CHECKING:  # pragma: no cover
        _document_class: Type[T]
        _all_document_fields: Set[str]
        _projected_fields: Set[str]

    def __init__(self) -> None:
        self._all_document_fields = set()
        self._projected_fields = self._all_document_fields.copy()

    def add_to_class(self, document_class: Type[T]) -> None:
        self._document_class = document_class  # noinspection
        self._all_document_fields = set(
            self._document_class.__fields_without_managers__.keys()
        )  # noinspection
        self._projected_fields = self._all_document_fields.copy()

    async def bulk_create(self, objs: List[T]) -> List[T]:
        """
        Bulk create a list of objects

        :rtype: List[T] Created objects with ids
        :param objs: list of objects to create
        """
        parsed_objs = list(map(lambda m: m.dict(), objs))
        results = await self._document_class.collection.insert_many(parsed_objs)

        results_objs = []
        for _id, obj in zip(results.inserted_ids, objs):
            obj.id = _id
            results_objs.append(obj)
        return results_objs

    async def bulk_delete(self, ids: List[Union[ObjectId, str]]) -> int:
        """delete many objects using ids

        :type ids: id of objects to delete
        :returns int number of deleted objects
        """

        delete_filter = {"_id": {"$in": ids}}
        results = await self._document_class.collection.delete_many(delete_filter)
        return results.deleted_count


class MongoBaseQueryManager(MongoBaseManager[T]):
    """query class responsible for building queries"""

    if TYPE_CHECKING:  # pragma: no cover
        _filter: Dict[str, Any]
        _result_cache: Optional[MongoCursor]
        _limit: Optional[int] = None
        _skip: Optional[int] = None

    def __init__(self) -> None:
        super(MongoBaseQueryManager, self).__init__()
        self._filter = {}
        self._limit = None
        self._skip = None
        self._result_cache = None

    def _check_fields_exist_in_document(self, fields: Set[str]) -> None:
        diff_fields = fields - self._all_document_fields
        if len(diff_fields):
            raise FieldNotFoundOnDocument(
                f"fields {diff_fields} are not found in the document"
                f" of type {self._document_class.__name__}"
            )

    def only(self, *fields: str) -> "MongoBaseQueryManager[T]":
        """only return fields when creating a query

        :param fields: allowed fields from projection
        :return: new query
        """
        fields_set = set(fields)
        self._check_fields_exist_in_document(fields_set)
        self._projected_fields = fields_set
        return self

    def exclude(self, *fields: str) -> "MongoBaseQueryManager[T]":
        """exclude fields from being returned when querying

        :param fields: excluded fields from projection
        :return: new query
        """
        fields_set = set(fields)
        self._check_fields_exist_in_document(fields_set)
        self._projected_fields = self._all_document_fields - fields_set
        return self

    def filter(self, **filter_kwargs: Any) -> "MongoBaseQueryManager[T]":
        """add the filter for mongodb find, filter result is not evaluated,
                and no db access will happen yet

        :param filter_kwargs: the filter to apply as a kwargs
        :return: new query
        """
        self._filter = filter_kwargs
        return self

    async def all(self) -> List[T]:
        """fetch all documents that matches filter

        :return: List[T]
        """
        async_cursor = MongoCursor(
            self._document_class,
            self._document_class.collection.delegate.find(
                self._filter,
                projection=list(self._projected_fields),
            ),
        )
        self._result_cache = async_cursor
        result = await async_cursor.skip(self._skip).to_list(length=self._limit)
        return result

    async def first(self) -> Optional[T]:
        """fetch the first document that matches filter, returns None if it doesn't exist
        :return: T
        """
        from motor_odm.documents import construct_document_with_default_values

        document_dict = await self._document_class.collection.find_one(
            self._filter,
            projection=list(self._projected_fields),
        )

        if document_dict is None:
            return None

        document = construct_document_with_default_values(
            document_dict, self._document_class
        )
        return document

    async def count(self) -> int:
        """
        count of document that matches the filter
        :return: int
        """
        params = {}
        if self._limit is not None:
            params["limit"] = self._limit

        if self._skip is not None:
            params["skip"] = self._skip

        return await self._document_class.collection.count_documents(
            self._filter, **params
        )

    def limit(self, count: int) -> "MongoBaseQueryManager[T]":
        """
        limit of the query
        :param count: max count
        :return: MongoBaseQueryBuilder
        """
        self._limit = count
        return self

    def skip(self, skip: int) -> "MongoBaseQueryManager[T]":
        """
        limit of the query
        :param skip: skip document
        :return: MongoBaseQueryBuilder
        """
        self._skip = skip
        return self

    async def get(self, **kwargs: Any) -> T:
        """get a document using its fields as filter options

        Document.objects.get(id="...")

        :raises DocumentDoestNotExists
        :param kwargs: fields
        :return: MongoDocument
        """

        if "id" in kwargs:  # allow to query by and the convert to _id
            kwargs["_id"] = kwargs.pop("id")
            if isinstance(kwargs["_id"], str):
                kwargs["_id"] = ObjectId(kwargs["_id"])

        result = await self._document_class.collection.find_one(kwargs)
        if result is None:
            raise DocumentDoestNotExists(f"Document with {kwargs} doesnt exists")
        return self._document_class(**result)


class MongoQueryManager(MongoBaseQueryManager[T]):
    """default query manager for MongoDocument"""
