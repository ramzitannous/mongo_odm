from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient
from mongo_odm.exceptions import ImproperlyConfigured

_motor_client: Optional[AsyncIOMotorClient] = None
_db_name: Optional[str] = None


def configure(motor_client: AsyncIOMotorClient, db_name: str) -> None:
    """
    Configure the used Motor client and database name,
    should be called before any database connection and early as possible
    especially before any document declaration

    :param motor_client: Instance of configured motor driver
    :param db_name: The name of the database used
    """
    global _motor_client, _db_name

    if motor_client is None:
        raise ImproperlyConfigured("Need to supply a configured motor client!")
    _motor_client = motor_client

    if db_name is None:
        raise ImproperlyConfigured(
            "need to supply a valid database name, None was given"
        )
    _db_name = db_name


def disconnect() -> None:
    """
    Disconnects database connection, should be called when webserver tears down
    """
    global _motor_client, _db_name

    if _motor_client is not None:
        _motor_client.close()


def get_motor_client() -> AsyncIOMotorClient:
    """
    Get the used Motor client
    """
    if _motor_client is None:
        raise ImproperlyConfigured(
            "should call configure with motor client instance before connecting to database"
        )
    return _motor_client


def get_db_name() -> str:
    """
    Get database used for connections
    """
    if _db_name is None:
        raise ImproperlyConfigured(
            "should call configure before connecting to database"
        )
    return _db_name
