from typing import Dict, Any
from pyspark.sql.types import DataType


class TypeMapper:
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
        return cls.POSTGRES_TO_SPARK.get(pg_type.lower(), "StringType")
    
    @classmethod
    def spark_to_clickhouse(cls, spark_type: str) -> str:
        return cls.SPARK_TO_CLICKHOUSE.get(spark_type, "String")
    
    @classmethod
    def validate_mapping(cls, source_type: str, target_type: str) -> bool:
        return True
