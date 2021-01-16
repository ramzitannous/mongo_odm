from typing import Any, List, TYPE_CHECKING, Type, Union

from bson import ObjectId

from motor_odm.exceptions import DocumentDoestNotExists

if TYPE_CHECKING:
    from motor_odm.documents import MongoDocument


class MongoBaseManager:
    """Base Manager that all type of managers should inherits
    to create a new manager:

    managers.py

    class CustomManager(MongoBaseManager):
        pass

    documents.py

    class Document(MongoDocument):
        custom_manager = CustomManager
    """

    if TYPE_CHECKING:
        _document_class: Type["MongoDocument"]

    def add_to_class(self, document_class: Type["MongoDocument"]) -> None:
        self._document_class = document_class


class MongoCrudManager(MongoBaseManager):
    async def bulk_create(self, objs: List["MongoDocument"]) -> List["MongoDocument"]:
        """
        Bulk create a list of objects
        :rtype: List["MongoDocument"] Created objects with ids
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


class MongoQueryManager(MongoBaseManager):
    """Responsible for all querying operations on a collection"""

    async def get(self, **kwargs: Any) -> "MongoDocument":
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

    async def first(self) -> "MongoDocument":
        pass


class MongoDefaultManager(MongoQueryManager, MongoCrudManager):
    """default manager for a document"""
