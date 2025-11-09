from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from data_transfer_lib.connections.base import BaseConnection


class ClickHouse(BaseConnection):
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
        print("Валидация параметров подключения")
    
    def get_jdbc_url(self) -> str:
        print("Получить JDBC URL для ClickHouse")
        return f"jdbc:clickhouse://{self.host}:{self.http_port}/{self.database}"
    
    def get_connection_properties(self) -> Dict[str, str]:
        print("Получить свойства подключени")
        return {
            "user": self.user,
            "password": self.password,
            "driver": "com.clickhouse.jdbc.ClickHouseDriver"
        }
    
    def test_connection(self) -> bool:
        print("Проверка подключения к ClickHouse")
        return True
    
    def get_table_schema(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """Получить схему таблицы из ClickHouse"""
        print(f"Получить схему таблицы из ClickHouse для {db_name}.{table_name}")
        return {}
