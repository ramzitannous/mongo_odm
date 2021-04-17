class ImproperlyConfigured(ValueError):
    """Exception for wrong configuration"""


class InvalidFieldType(TypeError):
    """Exception for invalid fields"""


class InvalidCollectionName(ValueError):
    """Exception for invalid collection name eg. $person"""


class InvalidFieldName(ValueError):
    """Exception for invalid field names eg. $hello"""


class DocumentDoestNotExists(Exception):
    """Exception happens when document doesnt exist in the db"""


class PrimaryKeyException(ValueError):
    """All Exceptions related to _id"""


class FieldNotFoundOnDocument(ValueError):
    """Raised when field is not found in the document"""


class PrimaryKeyCantBeExcluded(ValueError):
    """Raised when trying to exclude primary key"""
