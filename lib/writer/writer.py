from pyspark.sql import DataFrame
from data_transfer_lib.writer.base import BaseWriter
from data_transfer_lib.connections.base import BaseConnection
from data_transfer_lib.schema.validator import SchemaValidator


class Writer(BaseWriter):
    def __init__(
        self,
        connection: BaseConnection,
        db_name: str,
        table_name: str,
        if_exists: bool = True,
        **params
    ):
        self.target_schema = None
        super().__init__(
            connection=connection,
            db_name=db_name,
            table_name=table_name,
            if_exists=if_exists,
            **params
        )
    
    def _prepare(self) -> None:
        
        if self.if_exists:
            # Получаем схему целевой таблицы
            self.target_schema = self.connection.get_table_schema(
                self.db_name,
                self.table_name
            )
            print(f"Получена схема target таблицы {self.db_name}.{self.table_name}")
        else:
            print("ОШИБКА: Функционал создания таблицы пока не реализован")
    
    def start(self, df: DataFrame, **params) -> None:
        """
        Запуск загрузки данных в БД
        
        Args:
            df: Spark DataFrame для загрузки
            **params: Дополнительные параметры (например, batch_size, mode)
        """
        print(f"Вызван метод Writer.start для {self.db_name}.{self.table_name}")
        print(f"Параметры: {params}")
        
        # Получаем схему DataFrame
        df_schema = {}  # df.schema преобразованная в dict
        
        # Валидируем совместимость схем
        is_valid = SchemaValidator.validate_spark_to_target(
            df_schema,
            self.target_schema
        )
        
        if not is_valid:
            print("ОШИБКА: Невозможно записать данные без потери информации")
            return
        
        # Здесь будет логика записи через Spark JDBC
        # df.write.jdbc(...)
        
        print("Данные успешно загружены (заглушка)")