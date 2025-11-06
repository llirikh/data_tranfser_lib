"""
Коннектор для ClickHouse
"""

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from data_transfer_lib.connections.base import BaseConnection


class ClickHouse(BaseConnection):
    """
    Класс для подключения к ClickHouse
    """
    
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str = "default",
        port: int = 8123,
        http_port: int = 8123,
        native_port: int = 9000,
        spark: Optional[SparkSession] = None,
        **kwargs
    ):
        self.http_port = http_port
        self.native_port = native_port
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
        print("Вызван метод ClickHouse._validate_connection_params")
    
    def get_jdbc_url(self) -> str:
        """Получить JDBC URL для ClickHouse"""
        print("Вызван метод ClickHouse.get_jdbc_url")
        return f"jdbc:clickhouse://{self.host}:{self.http_port}/{self.database}"
    
    def get_connection_properties(self) -> Dict[str, str]:
        """Получить свойства подключения"""
        print("Вызван метод ClickHouse.get_connection_properties")
        return {
            "user": self.user,
            "password": self.password,
            "driver": "com.clickhouse.jdbc.ClickHouseDriver"
        }
    
    def test_connection(self) -> bool:
        """Проверка подключения к ClickHouse"""
        print("Вызван метод ClickHouse.test_connection")
        return True
    
    def get_table_schema(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """Получить схему таблицы из ClickHouse"""
        print(f"Вызван метод ClickHouse.get_table_schema для {db_name}.{table_name}")
        return {}
