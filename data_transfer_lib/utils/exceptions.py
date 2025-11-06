"""
Кастомные исключения библиотеки
"""

class DataTransferException(Exception):
    """Базовое исключение библиотеки"""
    pass


class ConnectionException(DataTransferException):
    """Исключения связанные с подключением"""
    pass


class SchemaValidationException(DataTransferException):
    """Исключения при валидации схемы"""
    pass


class TypeMappingException(DataTransferException):
    """Исключения при маппинге типов данных"""
    pass