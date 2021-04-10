from typing import (
    Generic,
    List,
    Optional,
    TypeVar,
)

from typing import Type, TYPE_CHECKING

from motor.motor_asyncio import AsyncIOMotorCursor
from pymongo.cursor import Cursor


if TYPE_CHECKING:
    from motor_odm.documents import MongoDocument  # noqa

T = TypeVar("T", bound="MongoDocument")


class MongoCursor(Generic[T], AsyncIOMotorCursor):
    if TYPE_CHECKING:  # pragma: no cover
        _document_class: Type[T]
        _cursor: Cursor

    def __init__(
        self,
        document_class: Type[T],
        cursor: Cursor,
    ):
        self._document_class = document_class
        self._cursor = cursor
        super().__init__(cursor, self._document_class.collection)

    async def next(self) -> T:
        from motor_odm.documents import construct_document_with_default_values

        # need to override the next to covert it to T
        dict_result = await super().next()
        return construct_document_with_default_values(dict_result, self._document_class)

    async def to_list(self, length: Optional[int]) -> List[T]:
        from motor_odm.documents import construct_document_with_default_values

        dict_documents = await super().to_list(length=length)
        return [
            construct_document_with_default_values(obj, self._document_class)
            for obj in dict_documents
        ]

    __anext__ = next
