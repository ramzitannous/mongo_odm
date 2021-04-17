import re

from mongo_odm.exceptions import InvalidCollectionName, InvalidFieldName

_ID = "_id"
_NEW_ID = "id"


def validate_field_name(name: str) -> None:
    # https://docs.mongodb.com/manual/reference/limits/#Restrictions-on-Field-Names
    if name.startswith("$"):
        raise InvalidFieldName(
            "key_name cannot start with the dollar sign ($) character"
        )

    if "." in name:
        raise InvalidFieldName("key_name cannot contain the dot (.) character")


def validate_collection_name(collection_name: str, cls_name: str) -> None:
    # https://docs.mongodb.com/manual/reference/limits/#Restriction-on-Collection-Names
    if "$" in collection_name:
        raise InvalidCollectionName(
            f"Invalid collection name for {cls_name}: cannot contain '$'"
        )

    if collection_name == "":
        raise InvalidCollectionName(
            f"Invalid collection name for {cls_name}: cannot be empty"
        )

    if collection_name.startswith("system."):
        raise InvalidCollectionName(
            f"Invalid collection name for {cls_name}:" " cannot start with 'system.'"
        )


def to_snake_case(s: str) -> str:
    tmp = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", s)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", tmp).lower()
