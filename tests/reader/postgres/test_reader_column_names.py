import pytest
from decimal import Decimal
from datetime import datetime

from data_transfer_lib import Reader
from .helpers import create_test_table, insert_test_data

class TestReaderColumnNames:
    
    def test_simple_column_names(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_simple_columns"
        create_test_table(
            pg_cursor,
            table_name,
            [
                ("id", "INTEGER"),
                ("name", "VARCHAR(100)"),
                ("age", "INTEGER"),
                ("salary", "NUMERIC(10, 2)")
            ]
        )
        
        insert_test_data(
            pg_cursor,
            table_name,
            ["id", "name", "age", "salary"],
            [
                (1, "Alice", 30, Decimal("50000.50")),
                (2, "Bob", 25, Decimal("45000.00"))
            ]
        )
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )
        
        df = reader.start()
        
        expected_columns = ["id", "name", "age", "salary"]
        actual_columns = df.columns
        assert sorted(actual_columns) == sorted(expected_columns)
    
    def test_mixed_case_column_names(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_mixed_case_columns"
        
        create_test_table(
            pg_cursor,
            table_name,
            [
                ("UserId", "INTEGER"),
                ("FirstName", "TEXT"),
                ("last_name", "TEXT"),
                ("CREATED_AT", "TIMESTAMP")
            ]
        )
        
        insert_test_data(
            pg_cursor,
            table_name,
            ["UserId", "FirstName", "last_name", "CREATED_AT"],
            [
                (1, "John", "Doe", datetime(2024, 1, 1, 12, 0, 0))
            ]
        )
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )

        df = reader.start()
        
        expected_columns = ["userid", "firstname", "last_name", "created_at"]
        actual_columns = [col.lower() for col in df.columns]
        
        assert sorted(actual_columns) == sorted(expected_columns)
