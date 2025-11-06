"""
Реализация Reader для чтения данных из БД
"""

from typing import Optional, Any
from pyspark.sql import DataFrame
from data_transfer_lib.reader.base import BaseReader
from data_transfer_lib.connections.base import BaseConnection
from data_transfer_lib.schema.validator import SchemaValidator


class Reader(BaseReader):
    """
    Класс для чтения данных из БД в Spark DataFrame
    """
    
    def __init__(
        self,
        connection: BaseConnection,
        db_name: str,
        table_name: str,
        **params
    ):
        self.source_schema = None
        super().__init__(
            connection=connection,
            db_name=db_name,
            table_name=table_name,
            **params
        )
    
    def _prepare(self) -> None:
        """Подготовка: получение и валидация схемы источника"""
        print("Вызван метод Reader._prepare")
        
        # Получаем схему таблицы из источника
        self.source_schema = self.connection.get_table_schema(
            self.db_name,
            self.table_name
        )
        
        # Валидируем возможность маппинга в Spark без потери данных
        is_valid = SchemaValidator.validate_source_to_spark(self.source_schema)
        
        if not is_valid:
            print("ОШИБКА: Невозможно загрузить данные без потери информации")
    
    def start(self, **params) -> DataFrame:
        """
        Запуск чтения данных из БД
        
        Args:
            **params: Дополнительные параметры (например, partition_column, num_partitions)
        
        Returns:
            DataFrame: Spark DataFrame с данными из источника
        """
        print(f"Вызван метод Reader.start для {self.db_name}.{self.table_name}")
        print(f"Параметры: {params}")
        
        # Здесь будет логика чтения через Spark JDBC
        # df = self.connection.spark.read.jdbc(...)
        
        return self.connection.spark.createDataFrame([], schema="id INT, name STRING")
