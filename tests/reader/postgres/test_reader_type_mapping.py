import pytest
from decimal import Decimal
from datetime import datetime, date

from data_transfer_lib import Reader
from .helpers import create_test_table, insert_test_data

class TestReaderTypeMapping:
    
    def test_boolean_type_mapping(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_boolean_mapping"
        
        create_test_table(
            pg_cursor,
            table_name,
            [("id", "INTEGER"), ("flag", "BOOLEAN")]
        )
        
        insert_test_data(pg_cursor, table_name, ["id", "flag"], [(1, True)])
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )
        
        df = reader.start()
        
        flag_type = [
            field.dataType for field in df.schema.fields if field.name == "flag"
        ][0]
        assert str(flag_type) == "BooleanType()"
    
    def test_integer_types_mapping(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_integer_mapping"
        
        create_test_table(
            pg_cursor,
            table_name,
            [
                ("id", "INTEGER"),
                ("small_val", "SMALLINT"),
                ("int_val", "INTEGER"),
                ("big_val", "BIGINT"),
                ("serial_val", "SERIAL"),
                ("bigserial_val", "BIGSERIAL")
            ]
        )
        
        insert_test_data(
            pg_cursor,
            table_name,
            ["id", "small_val", "int_val", "big_val"],
            [(1, 100, 1000, 10000)]
        )
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )
        
        df = reader.start()
        schema_dict = {field.name: str(field.dataType) for field in df.schema.fields}
        
        assert "ShortType()" in schema_dict["small_val"]
        assert "IntegerType()" in schema_dict["int_val"]
        assert "LongType()" in schema_dict["big_val"]
        assert "IntegerType()" in schema_dict["serial_val"]
        assert "LongType()" in schema_dict["bigserial_val"]
    
    def test_float_types_mapping(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_float_mapping"
        
        create_test_table(
            pg_cursor,
            table_name,
            [
                ("id", "INTEGER"),
                ("real_val", "REAL"),
                ("float_val", "FLOAT"),
                ("double_val", "DOUBLE PRECISION")
            ]
        )
        
        insert_test_data(
            pg_cursor,
            table_name,
            ["id", "real_val", "float_val", "double_val"],
            [(1, 3.14, 2.71, 1.41421356)]
        )
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )
        
        df = reader.start()
        schema_dict = {field.name: str(field.dataType) for field in df.schema.fields}
        
        assert "FloatType()" in schema_dict.get("real_val", "")
        assert "DoubleType()" in schema_dict.get("double_val", "")
    
    def test_decimal_type_mapping(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_decimal_mapping"
        
        create_test_table(
            pg_cursor,
            table_name,
            [
                ("id", "INTEGER"),
                ("decimal_val", "DECIMAL(10, 2)"),
                ("numeric_val", "NUMERIC(20, 5)")
            ]
        )
        
        insert_test_data(
            pg_cursor,
            table_name,
            ["id", "decimal_val", "numeric_val"],
            [(1, Decimal("123.45"), Decimal("12345.67890"))]
        )
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )
        
        df = reader.start()
        schema_dict = {field.name: str(field.dataType) for field in df.schema.fields}
        
        assert "DecimalType" in schema_dict["decimal_val"]
        assert "DecimalType" in schema_dict["numeric_val"]
    
    def test_string_types_mapping(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_string_mapping"
        
        create_test_table(
            pg_cursor,
            table_name,
            [
                ("id", "INTEGER"),
                ("varchar_col", "VARCHAR(100)"),
                ("char_col", "CHAR(10)"),
                ("text_col", "TEXT"),
                ("bpchar_col", "BPCHAR")
            ]
        )
        
        insert_test_data(
            pg_cursor,
            table_name,
            ["id", "varchar_col", "char_col", "text_col", "bpchar_col"],
            [(1, "test", "test", "test", "test")]
        )
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )
        
        df = reader.start()
        schema_dict = {field.name: str(field.dataType) for field in df.schema.fields}
        
        for col in ["varchar_col", "char_col", "text_col", "bpchar_col"]:
            assert "String" in schema_dict[col] or "Varchar" in schema_dict[col] or "Char" in schema_dict[col]
    
    def test_temporal_types_mapping(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_temporal_mapping"
        
        create_test_table(
            pg_cursor,
            table_name,
            [
                ("id", "INTEGER"),
                ("date_col", "DATE"),
                ("timestamp_col", "TIMESTAMP"),
                ("timestamptz_col", "TIMESTAMP WITH TIME ZONE")
            ]
        )
        
        insert_test_data(
            pg_cursor,
            table_name,
            ["id", "date_col", "timestamp_col", "timestamptz_col"],
            [(1, date(2024, 1, 1), datetime(2024, 1, 1, 12, 0, 0), datetime(2024, 1, 1, 12, 0, 0))]
        )
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )
        
        df = reader.start()
        schema_dict = {field.name: str(field.dataType) for field in df.schema.fields}
        
        assert "DateType()" in schema_dict["date_col"]
        assert "TimestampType()" in schema_dict["timestamp_col"]
        assert "TimestampType()" in schema_dict["timestamptz_col"]
