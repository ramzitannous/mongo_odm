import pytest
from bson import ObjectId
from motor_odm.exceptions import DocumentDoestNotExists
from tests.document import PersonDocument


@pytest.mark.asyncio
async def test_save(event_loop):
    p = PersonDocument(age=10, name="ramzi")
    await p.save()
    assert p.id is not None
    assert isinstance(p.id, ObjectId)


@pytest.mark.asyncio
async def test_document_reload():
    t = PersonDocument(age=10, name="test")
    await t.save()
    _id = t.id
    await t.reload()
    assert t.id == _id


@pytest.mark.asyncio
async def test_reload_document_not_exist():
    t = PersonDocument(age=10, name="test")
    with pytest.raises(DocumentDoestNotExists):
        await t.reload()
    await t.save()
    t.id = ObjectId("5349b4ddd2781d08c09890f3")
    with pytest.raises(DocumentDoestNotExists):
        await t.reload()


@pytest.mark.asyncio
async def test_update_if_document_exists(event_loop):
    p = PersonDocument(age=10, name="ramzi")
    await p.save()
    old_id = p.id
    assert p.id is not None
    p.age = 20
    await p.save()
    assert p.id == old_id


@pytest.mark.asyncio
async def test_delete_document(event_loop):
    p = PersonDocument(age=10, name="ramzi")
    await p.save()
    await p.delete()
    with pytest.raises(DocumentDoestNotExists):  # check it doesn't exists
        await p.delete()
