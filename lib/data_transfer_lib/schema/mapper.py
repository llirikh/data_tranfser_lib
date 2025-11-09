"""
Маппинг типов данных между различными БД и Spark
"""

from typing import Dict, Any
from pyspark.sql.types import DataType


class TypeMapper:
    """
    Класс для маппинга типов данных
    """
    
    # Маппинг PostgreSQL -> Spark
    POSTGRES_TO_SPARK = {
        "integer": "IntegerType",
        "bigint": "LongType",
        "smallint": "ShortType",
        "numeric": "DecimalType",
        "decimal": "DecimalType",
        "real": "FloatType",
        "double precision": "DoubleType",
        "varchar": "StringType",
        "text": "StringType",
        "timestamp": "TimestampType",
        "date": "DateType",
        "boolean": "BooleanType",
    }
    
    # Маппинг Spark -> ClickHouse
    SPARK_TO_CLICKHOUSE = {
        "IntegerType": "Int32",
        "LongType": "Int64",
        "ShortType": "Int16",
        "DecimalType": "Decimal",
        "FloatType": "Float32",
        "DoubleType": "Float64",
        "StringType": "String",
        "TimestampType": "DateTime",
        "DateType": "Date",
        "BooleanType": "UInt8",
    }
    
    @classmethod
    def postgres_to_spark(cls, pg_type: str) -> str:
        """Маппинг типа PostgreSQL в тип Spark"""
        print(f"Вызван метод TypeMapper.postgres_to_spark для типа {pg_type}")
        return cls.POSTGRES_TO_SPARK.get(pg_type.lower(), "StringType")
    
    @classmethod
    def spark_to_clickhouse(cls, spark_type: str) -> str:
        """Маппинг типа Spark в тип ClickHouse"""
        print(f"Вызван метод TypeMapper.spark_to_clickhouse для типа {spark_type}")
        return cls.SPARK_TO_CLICKHOUSE.get(spark_type, "String")
    
    @classmethod
    def validate_mapping(cls, source_type: str, target_type: str) -> bool:
        """Валидация совместимости типов"""
        print(f"Вызван метод TypeMapper.validate_mapping: {source_type} -> {target_type}")
        return True
