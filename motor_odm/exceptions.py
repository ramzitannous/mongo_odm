class ImproperlyConfigured(ValueError):
    """
    Exception for wrong configuration
    """


class InvalidFieldType(TypeError):
    """
    Exception for invalid fields
    """


class InvalidCollectionName(ValueError):
    """
    Exception for invalid collection name eg. $person
    """


class InvalidFieldName(ValueError):
    """
    Exception for invalid field names eg. $hello
    """
