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

    def seed_database(self, csv_path: str):
        """Читает CSV и заливает реальный датасет в Oracle (один раз)."""
        self.logger.info("Проверка наличия данных в Oracle...")
        try:
            df = self.extract_data(retries=1)
            if df.count() > 10:
                self.logger.info("Реальный датасет уже загружен в БД Oracle. Идем дальше.")
                return
        except Exception:
            pass

        self.logger.info(f"Таблица пуста. Начинаем заливку датасета {csv_path} в БД Oracle...")
        # Читаем исходный файл
        df_csv = self.spark.read.csv(
            csv_path, header=True, inferSchema=True, sep='\t', multiLine=True, escape='"'
        )

        df_to_db = df_csv.select(self.config.feature_columns).limit(50000)

        df_to_db.write \
            .format("jdbc") \
            .option("url", self.config.db_url) \
            .option("dbtable", self.config.source_table) \
            .option("user", self.config.db_user) \
            .option("password", self.config.db_password) \
            .option("driver", self.config.db_driver) \
            .mode("overwrite") \
            .save()

        self.logger.info("✅ Реальный датасет успешно загружен в Oracle!")

    def extract_data(self, retries=20) -> DataFrame:
        """Читает данные из Oracle (Extract)."""
        delay = 10
        for attempt in range(1, retries + 1):
            try:
                self.logger.info(f"Чтение таблицы {self.config.source_table} из БД...")
                df = self.spark.read \
                    .format("jdbc") \
                    .option("url", self.config.db_url) \
                    .option("dbtable", self.config.source_table) \
                    .option("user", self.config.db_user) \
                    .option("password", self.config.db_password) \
                    .option("driver", self.config.db_driver) \
                    .load()

                count = df.count()
                self.logger.info(f"Успешно выгружено {count} строк из Oracle.")
                return df
            except Py4JJavaError as e:
                if attempt == retries:
                    raise e
                error_msg = str(e.java_exception).split('\n')[0]
                self.logger.warning(f"БД Oracle еще грузится (Попытка {attempt}). Ошибка: {error_msg}")
                time.sleep(delay)

    def load_results(self, df: DataFrame) -> None:
        """Сохраняет результаты работы модели (Load)."""
        self.logger.info(f"Выгрузка результатов кластеризации в таблицу {self.config.target_table}...")

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

        self.logger.info("✅ Результаты (с кластерами) успешно сохранены в БД!")