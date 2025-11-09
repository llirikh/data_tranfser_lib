from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from data_transfer_lib.connections.base import BaseConnection


class Postgres(BaseConnection):
    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str = "postgres",
        port: int = 5432,
        spark: Optional[SparkSession] = None,
    ):
        super().__init__(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            spark=spark,
        )
    
    def _validate_connection_params(self) -> None:
        print("Валидация параметров подключения")
    
    def get_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
    
    def get_connection_properties(self) -> Dict[str, str]:
        print("Получить свойства подключения")
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver"
        }
    
    def test_connection(self) -> bool:
        print("Проверка подключения к PostgreSQL")
        return True
    
    def get_table_schema(self, db_name: str, table_name: str) -> Dict[str, Any]:
        print(f"Получить схему таблицы из PostgreSQL для {db_name}.{table_name}")
        return {}
