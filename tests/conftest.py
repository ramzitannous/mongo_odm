import asyncio

import pytest
from motor.motor_asyncio import AsyncIOMotorClient
from motor_odm import configure, disconnect
from pymongo import MongoClient

DB_NAME = "motor_test_db"

HOST = "localhost"
PORT = 27017

io_loop = asyncio.new_event_loop()
asyncio.set_event_loop(io_loop)
motor_client: MongoClient = AsyncIOMotorClient(host=HOST, port=PORT, io_loop=io_loop)
configure(motor_client, DB_NAME)


def clean_up(loop):
    disconnect()
    loop.run_until_complete(motor_client.drop_database(DB_NAME))


@pytest.fixture(scope="session")
def event_loop():
    configure(motor_client, DB_NAME)
    yield io_loop
    clean_up(io_loop)
    io_loop.close()
