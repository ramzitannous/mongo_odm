from typing import Generic, List, Optional, TypeVar

from typing import Type, TYPE_CHECKING

from motor.motor_asyncio import AsyncIOMotorCursor
from pymongo.cursor import Cursor


if TYPE_CHECKING:  # pragma: no cover
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
        # need to override the next to covert it to T
        dict_result = await super().next()
        return self._document_class.construct(**dict_result)

    async def to_list(self, length: Optional[int]) -> List[T]:
        dict_documents = await super().to_list(length=length)
        result = map(
            lambda obj: self._document_class.construct(**obj),
            dict_documents,
        )
        return list(result)

    __anext__ = next
