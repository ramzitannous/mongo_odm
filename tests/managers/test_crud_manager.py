import pytest

from motor_odm.documents import MongoDocument


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
