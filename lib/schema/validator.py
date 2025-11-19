from typing import Dict, Any
from pyspark.sql import DataFrame
from data_transfer_lib.utils.exceptions import SchemaValidationException
from data_transfer_lib.schema.mapper import TypeMapper


class SchemaValidator:

    @staticmethod
    def validate_source_to_spark(source_schema: Dict[str, Any]) -> bool:
        unsupported_types = []
        
        for column, pg_type in source_schema.items():
            base_type = pg_type.split('(')[0].strip().lower()
            spark_type = TypeMapper.postgres_to_spark(base_type)
            
            if spark_type == "StringType" and base_type not in TypeMapper.POSTGRES_TO_SPARK:
                unsupported_types.append(f"{column}: {pg_type}")
        
        if unsupported_types:
            error_msg = f"Error:\n Unsupported types detected: {', '.join(unsupported_types)}"
            raise SchemaValidationException(error_msg)
        
        print("Validation succeeded")

        return True
    
    @staticmethod
    def validate_spark_to_target(
        df_schema: Dict[str, Any],
        target_schema: Dict[str, Any]
    ) -> bool:
        print("Вызван метод SchemaValidator.validate_spark_to_target")
        return True
    
    @staticmethod
    def compare_schemas(
        source_schema: Dict[str, Any],
        target_schema: Dict[str, Any]
    ) -> Dict[str, Any]:
        print("Вызван метод SchemaValidator.compare_schemas")
        return {}
