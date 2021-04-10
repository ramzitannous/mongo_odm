from typing import List

import pytest

from motor_odm.cursor import MongoCursor
from motor_odm.documents import MongoDocument
from motor_odm.exceptions import DocumentDoestNotExists, FieldNotFoundOnDocument
from motor_odm.managers import MongoBaseQueryManager


class QueryTest(MongoDocument):
    name: str
    age: int
    salary: float


def test_only_fields():
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    query_manager = query_manager.only("name", "age")
    assert len(query_manager._projected_fields) == 2


def test_only_fields_that_doesnt_exists():
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)

    with pytest.raises(FieldNotFoundOnDocument):
        query_manager = query_manager.only("sa", "test")


def test_fields_that_doesnt_exists():
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)

    with pytest.raises(FieldNotFoundOnDocument):
        query_manager._check_fields_exist_in_document({"hello", "test"})


def test_all_fields_are_projected_by_default():
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)

    assert len(query_manager._projected_fields) == 4  # with id
    assert "name" in query_manager._projected_fields
    assert "age" in query_manager._projected_fields
    assert "salary" in query_manager._projected_fields
    assert len(query_manager._all_document_fields) == 4


def test_exclude_fields():
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)

    new_query_manager = query_manager.exclude("name", "age")
    assert len(new_query_manager._projected_fields) == 2
    assert "salary" in new_query_manager._projected_fields
    assert "id" in new_query_manager._projected_fields


def test_only_exclude_fields():
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    query_manager = query_manager.exclude("name", "age")
    query_manager = query_manager.only("name", "age")
    assert len(query_manager._projected_fields) == 2
    assert "name" in query_manager._projected_fields
    assert "age" in query_manager._projected_fields


def test_exclude_fields_that_doesnt_exists():
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    with pytest.raises(FieldNotFoundOnDocument):
        query_manager.exclude("sa", "test")


def test_filter():
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)

    new_query_manager = query_manager.filter(id="test id", age=10)
    assert new_query_manager._filter == {"id": "test id", "age": 10}


@pytest.mark.asyncio
async def test_first(event_loop):
    t1 = QueryTest(age=10, name="test1", salary=100)
    await t1.save()
    t2 = QueryTest(age=10, name="test2", salary=100)
    await t2.save()
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    first = await query_manager.filter(name="test1").first()
    assert isinstance(first, QueryTest)
    assert first.name == "test1"
    await t1.delete()
    await t2.delete()


@pytest.mark.asyncio
async def test_first_none(event_loop):
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    first = await query_manager.filter(name="test1").first()
    assert first is None


@pytest.mark.asyncio
async def test_all(event_loop):
    t1 = QueryTest(age=10, name="test1", salary=100)
    await t1.save()
    t2 = QueryTest(age=10, name="test2", salary=100)
    await t2.save()
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    objs = await query_manager.all()
    assert isinstance(objs, list)
    await t1.delete()
    await t2.delete()
    for obj in objs:
        assert obj.id is not None
        assert obj.name is not None
        assert obj.salary == 100
        assert obj.age == 10


@pytest.mark.asyncio
async def test_all_with_skip_and_limit(event_loop):
    objs = []
    for i in range(10):
        objs.append(QueryTest(age=10, name=f"test_{i}", salary=20))

    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    await query_manager.bulk_create(objs)
    objs = await query_manager.limit(9).skip(2).all()
    assert isinstance(objs, list)
    assert len(objs) == 8
    await query_manager.delete()

    objs = []
    for i in range(10):
        objs.append(QueryTest(age=10, name=f"test_{i}", salary=20))

    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    await query_manager.bulk_create(objs)
    objs = await query_manager.limit(9).skip(2).all()
    assert isinstance(objs, list)
    assert len(objs) == 8
    await query_manager.delete()


@pytest.mark.asyncio
async def test_raw_cursor_with_skip_and_limit(event_loop):
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    cursor = query_manager.limit(9).skip(2).raw_cursor()
    assert isinstance(cursor, MongoCursor)
    await query_manager.delete()


@pytest.mark.asyncio
async def test_raw_cursor_with_limit_only(event_loop):
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    cursor = query_manager.limit(9).raw_cursor()
    assert isinstance(cursor, MongoCursor)


@pytest.mark.asyncio
async def test_count_without_limit_skip_args(event_loop):
    t = QueryTest(age=10, name="test", salary=100)
    await t.save()
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    assert await query_manager.count() == 1
    await query_manager.delete()


@pytest.mark.asyncio
async def test_count_with_limit_skip_args(event_loop):
    objs = []
    for i in range(10):
        objs.append(QueryTest(age=10, name=f"test_{i}", salary=20))
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    await query_manager.bulk_create(objs)
    assert await query_manager.limit(10).skip(1).count() == 9
    await query_manager.delete()


@pytest.mark.asyncio
async def test_limit(event_loop):
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    new_manager = query_manager.limit(10)
    assert new_manager._limit == 10


@pytest.mark.asyncio
async def test_skip(event_loop):
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    new_query_manager = query_manager.skip(10)
    assert new_query_manager._skip == 10


@pytest.mark.asyncio
async def test_get(event_loop):
    t = QueryTest(age=10, name="test", salary=100)
    await t.save()
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    result = await query_manager.get(name="test")
    assert result.name == "test"
    await t.delete()


@pytest.mark.asyncio
async def test_get_by_id(event_loop):
    t = QueryTest(age=10, name="test1", salary=100)
    await t.save()
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    result = await query_manager.get(id=t.id)
    assert result.name == "test1"
    assert result.id == t.id
    await t.delete()


@pytest.mark.asyncio
async def test_get_by_id_str(event_loop):
    t = QueryTest(age=10, name="test2", salary=100)
    await t.save()
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    result = await query_manager.get(id=str(t.id))
    assert result.name == "test2"
    assert result.id == t.id
    await t.delete()


@pytest.mark.asyncio
async def test_get_exception_raised_when_notfound(event_loop):
    t = QueryTest(age=10, name="test", salary=100)
    await t.save()
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    with pytest.raises(DocumentDoestNotExists):
        result = await query_manager.get(name="Test")
    await t.delete()


@pytest.mark.asyncio
async def test_delete_by_filter(event_loop):
    q1 = QueryTest(age=10, name="test_0", salary=20)
    await q1.save()
    q2 = QueryTest(age=10, name="test_1", salary=20)
    await q2.save()
    query_manager = MongoBaseQueryManager()
    query_manager.add_to_class(QueryTest)
    count = await query_manager.filter(age=10).delete()
    assert count == 2


@pytest.mark.asyncio
async def test_debug_info(event_loop):
    class EmbeddedDocument(MongoDocument):
        salary: int

    class DebugDocument(MongoDocument):
        age: int
        name: str
        list: List[str]
        embedded: EmbeddedDocument

    query = MongoBaseQueryManager()
    query.add_to_class(DebugDocument)
    debug_info = query.limit(10).skip(2).exclude("embedded").filter(age=10).debug()
    assert debug_info == {
        "age": 10,
        "skip": 2,
        "limit": 10,
        "projection": {"list", "name", "id", "age"},
    }
