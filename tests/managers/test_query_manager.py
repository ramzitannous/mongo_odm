import pytest
from motor_odm.exceptions import DocumentDoestNotExists
from tests.document import PersonDocument


@pytest.mark.asyncio
async def test_get_document(event_loop):
    p = PersonDocument(age=10, name="test_2")
    await p.save()
    result = await PersonDocument.objects.get(id=p.id)
    assert result.id == p.id
    r = await PersonDocument.objects.get(name="test_2")
    assert r.id == p.id
    await p.delete()


@pytest.mark.asyncio
async def test_get_by_str_id(event_loop):
    p = PersonDocument(age=10, name="test")
    await p.save()
    result = await PersonDocument.objects.get(id=str(p.id))
    assert result.id == p.id
    await p.delete()


@pytest.mark.asyncio
async def test_get_document_doesnt_exists(event_loop):
    p = PersonDocument(age=10, name="test")
    await p.save()
    with pytest.raises(DocumentDoestNotExists):
        await PersonDocument.objects.get(id="5349b4ddd2781d08c09890f5")
