import pytest

from mongo_odm.documents import MongoDocument


@pytest.mark.asyncio
async def test_create_many_objects(event_loop):
    class CreateTest(MongoDocument):
        name: str

    objects = []
    for i in range(6):
        objects.append(CreateTest(name=f"test_{i}"))

    new_objects = await CreateTest.objects.bulk_create(objects)
    for obj in new_objects:
        assert obj.id is not None


@pytest.mark.asyncio
async def test_delete_many_objects(event_loop):
    class DeleteTest(MongoDocument):
        name: str

    objects = []
    for i in range(6):
        objects.append(DeleteTest(name=f"test_{i}"))

    new_objects = await DeleteTest.objects.bulk_create(objects)
    ids = map(lambda o: o.id, new_objects)
    count = await DeleteTest.objects.bulk_delete(list(ids))
    assert count == 6
