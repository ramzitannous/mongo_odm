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
            if isinstance(v, ObjectId) and ObjectId.is_valid(v):
                return v
            return ObjectId(str(v))
        except InvalidId:
            raise InvalidFieldType("Not a valid ObjectId")

    @classmethod
    def __modify_schema__(cls, field_schema: dict) -> None:
        field_schema.update(type="string")
