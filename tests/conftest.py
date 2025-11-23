import pytest

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from pyspark.sql import SparkSession

from data_transfer_lib import Postgres, Reader


@pytest.fixture(scope="session")
def spark_session():

    spark = (
        SparkSession.builder
        .appName("test")
        .master("spark://spark-master:7077")
        .config(
            "spark.jars.packages",
            ",".join([
                # clickhouse drivers
                "com.clickhouse:clickhouse-jdbc:0.7.2",
                # postgres drivers
                "org.postgresql:postgresql:42.7.3"
            ])
        )
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="session")
def postgres_connection(spark_session):

    postgres_conn = Postgres(
        host="postgres",
        user="postgres_user",
        password="postgres_password",
        database="postgres",
        port=5432,
        spark=spark_session
    )
    
    yield postgres_conn


@pytest.fixture(scope="session")

def pg_direct_connection():
    conn = psycopg2.connect(
        host="postgres",
        user="postgres_user",
        password="postgres_password",
        database="postgres",
        port=5432
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    
    yield conn
    
    conn.close()


@pytest.fixture(scope="session")
def test_schema(pg_direct_connection):

    cursor = pg_direct_connection.cursor()
    
    cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS test_schema;
    """)
    
    yield "test_schema"
    
    cursor.execute("DROP SCHEMA IF EXISTS test_schema CASCADE;")
    cursor.close()


@pytest.fixture
def pg_cursor(pg_direct_connection, test_schema):

    cursor = pg_direct_connection.cursor()
    
    yield cursor
    
    cursor.close()