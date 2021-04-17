import asyncio

from mongo_odm.config import configure
from motor.motor_asyncio import AsyncIOMotorClient

from mongo_odm.documents import MongoDocument

DB_NAME = "example_db"

HOST = "localhost"
PORT = 27017

io_loop = asyncio.new_event_loop()
asyncio.set_event_loop(io_loop)
motor_client = AsyncIOMotorClient(host=HOST, port=PORT, io_loop=io_loop)
configure(motor_client, DB_NAME)


class Person(MongoDocument["Person"]):
    name: str
    age: int
    salary: float


async def main() -> None:
    example = Person(name="ramzi", age=10, salary=100)
    await example.save()
    example_fetch = await Person.objects.only("name", "age").filter(salary=100).all()
    assert example_fetch is not None
    print(example_fetch)


asyncio.set_event_loop(io_loop)
io_loop.run_until_complete(main())
