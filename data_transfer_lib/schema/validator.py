"""
Валидатор схем данных
"""

from typing import Dict, Any
from pyspark.sql import DataFrame
from data_transfer_lib.utils.exceptions import SchemaValidationException


class SchemaValidator:
    """
    Класс для валидации схем
    """
    
    @staticmethod
    def validate_source_to_spark(source_schema: Dict[str, Any]) -> bool:
        """
        Проверка возможности загрузки из источника в Spark без потери данных
        """
        print("Вызван метод SchemaValidator.validate_source_to_spark")
        return True
    
    @staticmethod
    def validate_spark_to_target(
        df_schema: Dict[str, Any],
        target_schema: Dict[str, Any]
    ) -> bool:
        """
        Проверка возможности записи из Spark в target без потери данных
        """
        print("Вызван метод SchemaValidator.validate_spark_to_target")
        return True
    
    @staticmethod
    def compare_schemas(
        source_schema: Dict[str, Any],
        target_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Сравнение двух схем и возврат различий
        """
        print("Вызван метод SchemaValidator.compare_schemas")
        return {}
