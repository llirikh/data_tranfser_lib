"""
Коннектор для PostgreSQL
"""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from data_transfer_lib.connections.base import BaseConnection


class Postgres(BaseConnection):
    """
    Класс для подключения к PostgreSQL
    """
    
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str = "postgres",
        port: int = 5432,
        spark: Optional[SparkSession] = None,
        **kwargs
    ):
        super().__init__(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            spark=spark,
            **kwargs
        )
    
    def _validate_connection_params(self) -> None:
        """Валидация параметров подключения"""
        print("Вызван метод Postgres._validate_connection_params")
    
    def get_jdbc_url(self) -> str:
        """Получить JDBC URL для PostgreSQL"""
        print("Вызван метод Postgres.get_jdbc_url")
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
    
    def get_connection_properties(self) -> Dict[str, str]:
        """Получить свойства подключения"""
        print("Вызван метод Postgres.get_connection_properties")
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver"
        }
    
    def test_connection(self) -> bool:
        """Проверка подключения к PostgreSQL"""
        print("Вызван метод Postgres.test_connection")
        return True
    
    def get_table_schema(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """Получить схему таблицы из PostgreSQL"""
        print(f"Вызван метод Postgres.get_table_schema для {db_name}.{table_name}")
        return {}
