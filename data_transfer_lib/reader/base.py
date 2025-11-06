"""
Базовый класс для чтения данных
"""

from abc import ABC, abstractmethod
from typing import Optional, Any
from pyspark.sql import DataFrame
from data_transfer_lib.connections.base import BaseConnection


class BaseReader(ABC):
    """
    Абстрактный базовый класс для чтения данных
    """
    
    def __init__(
        self,
        connection: BaseConnection,
        db_name: str,
        table_name: str,
        **kwargs
    ):
        self.connection = connection
        self.db_name = db_name
        self.table_name = table_name
        self.extra_params = kwargs
        
        print(f"Инициализирован {self.__class__.__name__}")
        self._prepare()
    
    @abstractmethod
    def _prepare(self) -> None:
        """Подготовка к чтению (получение схемы, валидация)"""
        pass
    
    @abstractmethod
    def start(self, **params) -> DataFrame:
        """Запуск процесса чтения данных"""
        pass