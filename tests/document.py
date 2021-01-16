from motor_odm.documents import MongoDocument

ID = "5349b4ddd2781d08c09890f3"


class PersonDocument(MongoDocument):
    age: int
    name: str
