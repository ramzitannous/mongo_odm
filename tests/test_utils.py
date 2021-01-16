import motor_odm.utils as utils
import pytest
from motor_odm.exceptions import InvalidCollectionName, InvalidFieldName


@pytest.mark.parametrize("name", ("$test", "tes$t", "", "system.test"))
def test_invalid_collection_name(name: str):
    with pytest.raises(InvalidCollectionName):
        utils.validate_collection_name(name, name.upper())


@pytest.mark.parametrize("name", ("$test", "test.", "system.test"))
def test_invalid_field_name(name: str):
    with pytest.raises(InvalidFieldName):
        utils.validate_field_name(name)


@pytest.mark.parametrize(
    ("name", "expected"),
    (("Test", "test"), ("TestName", "test_name"), ("TEST_NAME", "test_name")),
)
def test_to_snake_conversion(name: str, expected: str):
    assert utils.to_snake_case(name) == expected
