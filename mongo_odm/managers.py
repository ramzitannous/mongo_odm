import copy
import logging
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    TYPE_CHECKING,
    Type,
    TypeVar,
    Union,
)

from bson import ObjectId

from mongo_odm.cursor import MongoCursor
from mongo_odm.exceptions import DocumentDoestNotExists, PrimaryKeyCantBeExcluded

if TYPE_CHECKING:  # pragma: no cover
    from mongo_odm.documents import MongoDocument  # noqa

T = TypeVar("T", bound="MongoDocument")

logger = logging.getLogger("manager")

_ID = "_id"
ID = "id"


class MongoBaseManager(Generic[T]):
    """Base Manager that all type of managers should inherit
    to create a new manager:

    managers.py

    class CustomManager(MongoBaseManager["CustomManager]):
        pass

    documents.py

    class Document(MongoDocument):
        custom_manager = CustomManager()
    """

    if TYPE_CHECKING:  # pragma: no cover
        _document_class: Type[T]

    def add_to_class(self, document_class: Type[T]) -> None:
        self._document_class = document_class  # noinspection

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
        :returns int, number of deleted objects
        """

        delete_filter = {"_id": {"$in": ids}}
        results = await self._document_class.collection.delete_many(delete_filter)
        return results.deleted_count


class MongoBaseQueryManager(MongoBaseManager[T]):
    """query manager responsible for building queries"""

    if TYPE_CHECKING:  # pragma: no cover
        _filter: Dict[str, Any]
        _result_cache: Optional[MongoCursor]
        _limit: Optional[int] = None
        _skip: Optional[int] = None
        _projected_fields: Optional[Dict[str, int]]

    def __init__(self) -> None:
        super(MongoBaseQueryManager, self).__init__()
        self._filter = {}
        self._limit = None
        self._skip = None
        self._result_cache = None
        self._projected_fields = None

    def _clone(self) -> "MongoBaseQueryManager[T]":
        """create a new MongoBaseQueryManager quickly"""
        new_manager: MongoBaseQueryManager[T] = MongoBaseQueryManager()
        new_manager._filter = self._filter.copy()
        new_manager._limit = self._limit
        new_manager._skip = self._skip
        new_manager._projected_fields = copy.copy(self._projected_fields)
        new_manager._document_class = self._document_class
        return new_manager

    def only(self, *fields: str) -> "MongoBaseQueryManager[T]":
        """only return fields when creating a query

        :param fields: allowed fields from projection
        :return: new query
        """
        fields_set = set(fields)
        new_manager = self._clone()

        new_manager._projected_fields = new_manager._projected_fields or {}
        for field_name in fields_set:
            new_manager._projected_fields[field_name] = 1

        return new_manager

    def exclude(self, *fields: str) -> "MongoBaseQueryManager[T]":
        """exclude fields from being returned when querying

        :param fields: excluded fields from projection
        :return: new query
        """
        fields_set = set(fields)
        if _ID in fields:
            raise PrimaryKeyCantBeExcluded('primary key "_id" cant be excluded')
        new_manager = self._clone()
        new_manager._projected_fields = new_manager._projected_fields or {}

        for field_name in fields_set:
            new_manager._projected_fields[field_name] = 0
        return new_manager

    def filter(self, **filter_kwargs: Any) -> "MongoBaseQueryManager[T]":
        """add the filter for mongodb find, filter result is not evaluated,
                and no db access will happen yet

        :param filter_kwargs: the filter to apply as a kwargs
        :return: new query
        """
        new_manager = self._clone()
        new_manager._filter = filter_kwargs
        return new_manager

    def raw_cursor(self) -> MongoCursor[T]:
        """fetch all documents that matches filter and return a raw cursor

        :return: MongoCursor[T]
        """
        async_cursor = MongoCursor(
            self._document_class,
            self._document_class.collection.delegate.find(
                self._filter, projection=self._projected_fields
            ),
        )
        if self._skip is not None:
            async_cursor = async_cursor.skip(self._skip)
        if self._limit is not None:
            async_cursor = async_cursor.limit(self._limit)
        self._result_cache = async_cursor

        return async_cursor

    async def all(self) -> List[T]:
        """fetch all documents that matches filter

        :return: List[T]
        """
        async_cursor = MongoCursor(
            self._document_class,
            self._document_class.collection.delegate.find(
                self._filter,
                projection=self._projected_fields,
            ),
        )
        self._result_cache = async_cursor
        if self._skip is not None:
            result = await async_cursor.skip(self._skip).to_list(length=self._limit)
        else:
            result = await async_cursor.to_list(length=self._limit)
        return result

    async def first(self) -> Optional[T]:
        """fetch the first document that matches filter, returns None if it doesn't exist
        :return: T
        """
        document_dict = await self._document_class.collection.find_one(
            self._filter,
            projection=self._projected_fields,
        )

        if document_dict is None:
            return None

        document = self._document_class.construct(**document_dict)
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
        new_manager = self._clone()
        new_manager._limit = count
        return new_manager

    def skip(self, skip: int) -> "MongoBaseQueryManager[T]":
        """
        limit of the query
        :param skip: skip document
        :return: MongoBaseQueryBuilder
        """
        new_manager = self._clone()
        new_manager._skip = skip
        return new_manager

    async def get(self, **kwargs: Any) -> T:
        """get a document using its fields as filter options

        Document.objects.get(id="...")

        :raises DocumentDoestNotExists
        :param kwargs: fields
        :return: MongoDocument
        """

        if ID in kwargs:  # allow to query by and the convert to _id
            kwargs[_ID] = kwargs.pop(ID)
            if isinstance(kwargs[_ID], str):
                kwargs[_ID] = ObjectId(kwargs[_ID])

        result = await self._document_class.collection.find_one(
            kwargs,
            projection=self._projected_fields,
        )
        if result is None:
            raise DocumentDoestNotExists(f"Document with {kwargs} doesnt exists")
        return self._document_class(**result)

    async def delete(self) -> int:
        """delete all documents that matches filter

        :return: int, number of deleted documents
        """
        result = await self._document_class.collection.delete_many(self._filter)
        return result.deleted_count

    def debug(self) -> dict:
        """log all fields for debug purpose"""
        debug_info = {
            **self._filter,
            "skip": self._skip,
            "limit": self._limit,
            "projection": self._projected_fields,
        }
        logger.debug(debug_info)
        return debug_info


class MongoQueryManager(MongoBaseQueryManager[T]):
    """default query manager for MongoDocument"""
