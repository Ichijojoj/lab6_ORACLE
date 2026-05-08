import logging
import time
from pyspark.sql import SparkSession, DataFrame
from py4j.protocol import Py4JJavaError


class OracleManager:
    """Управление взаимодействием с БД Oracle (Extract & Load)."""

    def __init__(self, spark: SparkSession, config):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    def extract_data(self) -> DataFrame:
        """Читает данные из Oracle с механизмом Retry (базе нужно время на запуск)."""
        retries = 20  # Увеличили до 20 попыток
        delay = 15  # Увеличили задержку до 15 секунд (Итого до 5 минут ожидания)

        for attempt in range(1, retries + 1):
            try:
                self.logger.info(f"Попытка {attempt}/{retries}: Чтение таблицы {self.config.source_table}...")
                df = self.spark.read \
                    .format("jdbc") \
                    .option("url", self.config.db_url) \
                    .option("dbtable", self.config.source_table) \
                    .option("user", self.config.db_user) \
                    .option("password", self.config.db_password) \
                    .option("driver", self.config.db_driver) \
                    .load()

                # Принудительное действие (action) для проверки реального подключения
                count = df.count()
                self.logger.info(f"Успешно загружено {count} строк из Oracle.")
                return df

            except Py4JJavaError as e:
                error_msg = str(e.java_exception).split('\n')[0]
                self.logger.warning(f"БД Oracle еще не готова (Попытка {attempt}). Ошибка: {error_msg}")
                time.sleep(delay)

        raise ConnectionError("Не удалось подключиться к Oracle после всех попыток. Проверьте состояние БД.")

    def load_results(self, df: DataFrame) -> None:
        """Сохраняет результаты работы модели обратно в Oracle."""
        self.logger.info(f"Выгрузка результатов в таблицу {self.config.target_table}...")

        # Oracle не понимает Spark Vectors. Удаляем служебные колонки перед записью.
        columns_to_drop = ["raw_features", "features"]
        df_to_write = df.drop(*columns_to_drop)

        df_to_write.write \
            .format("jdbc") \
            .option("url", self.config.db_url) \
            .option("dbtable", self.config.target_table) \
            .option("user", self.config.db_user) \
            .option("password", self.config.db_password) \
            .option("driver", self.config.db_driver) \
            .mode("overwrite") \
            .save()

        self.logger.info("Результаты успешно сохранены в БД!")