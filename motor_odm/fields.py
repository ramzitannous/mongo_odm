from typing import Callable, Generator

from bson import ObjectId
from bson.errors import InvalidId
from motor_odm.exceptions import InvalidFieldType


class PrimaryID(str):
    @classmethod
    def __get_validators__(
        cls,
    ) -> Generator[Callable[[str], ObjectId], None, None]:
        yield cls.validate

    @classmethod
    def validate(cls, v: str) -> ObjectId:
        try:
            return ObjectId(str(v))
        except InvalidId:
            raise InvalidFieldType("Not a valid ObjectId")
