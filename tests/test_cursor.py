import pytest
from tests.document import PersonDocument

from motor_odm.cursor import MongoCursor


async def setup_data(count=1):
    objs = []
    for i in range(count):
        objs.append(PersonDocument(age=10, name=f"test_{i}"))
    await PersonDocument.objects.bulk_create(objs)
    return objs


@pytest.mark.asyncio
async def test_cursor_next(event_loop):
    await setup_data()
    cursor = MongoCursor(
        PersonDocument,
        PersonDocument.collection.delegate.find(
            {},
            projection=["age", "name"],
        ),
    )
    async for obj in cursor:
        assert isinstance(obj, PersonDocument)
    await PersonDocument.objects.delete()


@pytest.mark.asyncio
async def test_cursor_to_list(event_loop):
    await setup_data()
    cursor = MongoCursor(
        PersonDocument,
        PersonDocument.collection.delegate.find(
            {},
            projection=["age", "name"],
        ),
    )
    result = await cursor.to_list(1)
    assert len(result) == 1
    assert isinstance(result, list)
    assert isinstance(result[0], PersonDocument)
    await PersonDocument.objects.delete()
