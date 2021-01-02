from motor_odm.documents import MongoDocument


class PersonDocument(MongoDocument):
    age: int
    name: str
