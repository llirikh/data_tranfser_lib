from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession


class BaseConnection(ABC):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: Optional[str] = None,
        spark: Optional[SparkSession] = None,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.spark = spark or self._get_or_create_spark()
        
        print(f"Инициализирован {self.__class__.__name__}")
        self._validate_connection_params()
    
    @abstractmethod
    def _validate_connection_params(self) -> None:
        """Валидация параметров подключения"""
        pass
    
    @abstractmethod
    def get_jdbc_url(self) -> str:
        """Получить JDBC URL для подключения"""
        pass
    
    @abstractmethod
    def get_connection_properties(self) -> Dict[str, str]:
        """Получить свойства подключения"""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Проверка подключения к БД"""
        pass
    
    @abstractmethod
    def get_table_schema(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """Получить схему таблицы из БД"""
        pass
    
    def _get_or_create_spark(self) -> SparkSession:
        """Получить или создать Spark сессию"""
        print("Вызван метод _get_or_create_spark")
        return SparkSession.builder.getOrCreate()