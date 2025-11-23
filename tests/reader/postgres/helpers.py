from typing import List, Tuple, Any
from data_transfer_lib import Reader

def create_test_table(cursor, table_name, columns_definition):

    columns = ", ".join([f"{col_name} {col_type}" for col_name, col_type in columns_definition])
    
    cursor.execute(f"""
        DROP TABLE IF EXISTS test_schema.{table_name};
    """)
    
    cursor.execute(f"""
        CREATE TABLE test_schema.{table_name} (
            {columns}
        );
    """)


def insert_test_data(cursor, table_name, columns, values):

    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    
    cursor.executemany(
        f"INSERT INTO test_schema.{table_name} ({columns_str}) VALUES ({placeholders})",
        values
    )