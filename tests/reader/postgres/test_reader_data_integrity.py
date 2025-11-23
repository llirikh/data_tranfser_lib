import pytest
from decimal import Decimal
from datetime import datetime, date

from data_transfer_lib import Reader
from .helpers import create_test_table, insert_test_data

class TestReaderDataIntegrity:
    def test_integer_data_integrity(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_integer_data"
        
        create_test_table(
            pg_cursor,
            table_name,
            [
                ("id", "INTEGER"),
                ("small_val", "SMALLINT"),
                ("big_val", "BIGINT")
            ]
        )
        
        test_data = [
            (1, 100, 9223372036854775807),  # max bigint
            (2, -32768, -9223372036854775808),  # min values
            (3, 32767, 0)
        ]
        
        insert_test_data(
            pg_cursor,
            table_name,
            ["id", "small_val", "big_val"],
            test_data
        )
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )
        
        df = reader.start()
        collected_data = df.orderBy("id").collect()
        
        for i, row in enumerate(collected_data):
            assert row["id"] == test_data[i][0]
            assert row["small_val"] == test_data[i][1]
            assert row["big_val"] == test_data[i][2]
    
    def test_decimal_data_integrity(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_decimal_data"
        
        create_test_table(
            pg_cursor,
            table_name,
            [
                ("id", "INTEGER"),
                ("price", "NUMERIC(10, 2)"),
                ("precise_val", "NUMERIC(20, 5)")
            ]
        )
        
        test_data = [
            (1, Decimal("12345.67"), Decimal("123456789012345.12345")),
            (2, Decimal("0.01"), Decimal("0.00001")),
            (3, Decimal("-999.99"), Decimal("-999.99999"))
        ]
        
        insert_test_data(
            pg_cursor,
            table_name,
            ["id", "price", "precise_val"],
            test_data
        )
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )
        
        df = reader.start()
        collected_data = df.orderBy("id").collect()
        
        for i, row in enumerate(collected_data):
            assert row["id"] == test_data[i][0]
            assert row["price"] == test_data[i][1]
            assert row["precise_val"] == test_data[i][2]
    
    def test_string_data_integrity(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_string_data"
        
        create_test_table(
            pg_cursor,
            table_name,
            [
                ("id", "INTEGER"),
                ("varchar_col", "VARCHAR(50)"),
                ("char_col", "CHAR(10)"),
                ("text_col", "TEXT")
            ]
        )
        
        test_data = [
            (1, "Hello", "ABCDE", "Long text with unicode: Привет мир"),
            (2, "Special: !@#$%", "12345", "Another text"),
            (3, "", "     ", "")
        ]
        
        insert_test_data(
            pg_cursor,
            table_name,
            ["id", "varchar_col", "char_col", "text_col"],
            test_data
        )
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )
        
        df = reader.start()
        collected_data = df.orderBy("id").collect()
        
        for i, row in enumerate(collected_data):
            assert row["id"] == test_data[i][0]
            assert row["varchar_col"] == test_data[i][1]
            assert row["char_col"].rstrip() == test_data[i][2].rstrip()
            assert row["text_col"] == test_data[i][3]
    
    def test_temporal_data_integrity(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_temporal_data"
        
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
        
        test_data = [
            (
                1,
                date(2024, 1, 15),
                datetime(2024, 1, 15, 10, 30, 45),
                datetime(2024, 1, 15, 10, 30, 45)
            ),
            (
                2,
                date(2000, 12, 31),
                datetime(2000, 12, 31, 23, 59, 59),
                datetime(2000, 12, 31, 23, 59, 59)
            )
        ]
        
        insert_test_data(
            pg_cursor,
            table_name,
            ["id", "date_col", "timestamp_col", "timestamptz_col"],
            test_data
        )
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )
        
        df = reader.start()
        collected_data = df.orderBy("id").collect()
        
        for i, row in enumerate(collected_data):
            assert row["id"] == test_data[i][0]
            assert row["date_col"] == test_data[i][1]
            
            assert abs((row["timestamp_col"] - test_data[i][2]).total_seconds()) < 1
            assert abs((row["timestamptz_col"] - test_data[i][3]).total_seconds()) < 1
    
    def test_boolean_data_integrity(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_boolean_data"
        
        create_test_table(
            pg_cursor,
            table_name,
            [
                ("id", "INTEGER"),
                ("is_active", "BOOLEAN"),
                ("is_deleted", "BOOLEAN")
            ]
        )
        
        test_data = [
            (1, True, False),
            (2, False, True),
            (3, True, True),
            (4, False, False)
        ]
        
        insert_test_data(
            pg_cursor,
            table_name,
            ["id", "is_active", "is_deleted"],
            test_data
        )
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )
        
        df = reader.start()
        collected_data = df.orderBy("id").collect()
        
        for i, row in enumerate(collected_data):
            assert row["id"] == test_data[i][0]
            assert row["is_active"] == test_data[i][1]
            assert row["is_deleted"] == test_data[i][2]