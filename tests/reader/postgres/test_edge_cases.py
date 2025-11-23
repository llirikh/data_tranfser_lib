import pytest
from datetime import date

from data_transfer_lib import Reader

from .helpers import create_test_table, insert_test_data


class TestReaderEdgeCases:
    
    def test_null_values(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_null_values"
        
        create_test_table(
            pg_cursor,
            table_name,
            [
                ("id", "INTEGER"),
                ("nullable_int", "INTEGER"),
                ("nullable_text", "TEXT"),
                ("nullable_date", "DATE")
            ]
        )
        
        insert_test_data(
            pg_cursor,
            table_name,
            ["id", "nullable_int", "nullable_text", "nullable_date"],
            [
                (1, None, None, None),
                (2, 100, "test", date(2024, 1, 1))
            ]
        )
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )
        
        df = reader.start()
        collected_data = df.orderBy("id").collect()
        
        assert collected_data[0]["nullable_int"] is None
        assert collected_data[0]["nullable_text"] is None
        assert collected_data[0]["nullable_date"] is None
        
        assert collected_data[1]["nullable_int"] == 100
        assert collected_data[1]["nullable_text"] == "test"
        assert collected_data[1]["nullable_date"] == date(2024, 1, 1)
    
    def test_empty_table(self, postgres_connection, pg_cursor, test_schema):
        table_name = "test_empty_table"
        
        create_test_table(
            pg_cursor,
            table_name,
            [
                ("id", "INTEGER"),
                ("name", "TEXT")
            ]
        )
        
        reader = Reader(
            connection=postgres_connection,
            db_name=test_schema,
            table_name=table_name
        )
        
        df = reader.start()
        
        assert df.count() == 0
        assert len(df.columns) == 2