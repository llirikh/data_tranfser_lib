"""
Базовый класс для загрузки данных
"""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from data_transfer_lib.connections.base import BaseConnection


class BaseWriter(ABC):
    """
    Абстрактный базовый класс для загрузки данных
    """
    
    def __init__(
        self,
        connection: BaseConnection,
        db_name: str,
        table_name: str,
        if_exists: bool = True,
        **kwargs
    ):
        self.connection = connection
        self.db_name = db_name
        self.table_name = table_name
        self.if_exists = if_exists
        self.extra_params = kwargs
        
        print(f"Инициализирован {self.__class__.__name__}")
        self._prepare()
    
    @abstractmethod
    def _prepare(self) -> None:
        """Подготовка к записи (получение схемы target, валидация)"""
        pass
    
    @abstractmethod
    def start(self, df: DataFrame, **params) -> None:
        """Запуск процесса загрузки данных"""
        pass