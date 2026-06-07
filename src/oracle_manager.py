import logging
import time
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from py4j.protocol import Py4JJavaError
from pyspark.errors import AnalysisException


class OracleManager:
    """Управление взаимодействием с БД Oracle (Extract & Load)."""

    def __init__(self, spark: SparkSession, config):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    def _verify_insert_format(self, df: DataFrame, expected_columns: list) -> bool:
        """
        Верификация формата записи (схемы данных) перед отправкой INSERT/Save в БД.
        Проверяет структуру, типы данных и наличие недопустимых пустых значений.
        """
        self.logger.info("Запуск проверки формата записи (валидация схемы)...")
        df_cols = set(df.columns)

        # 1. Проверка наличия всех ожидаемых полей
        for col_name in expected_columns:
            if col_name not in df_cols:
                self.logger.error(f"Ошибка валидации: В датафрейме отсутствует колонка {col_name}!")
                return False

        # 2. Проверка типов данных (должны быть числовыми)
        valid_types = ("DoubleType", "FloatType", "IntegerType", "LongType", "DecimalType")
        for field in df.schema.fields:
            if field.name in expected_columns:
                type_name = type(field.dataType).__name__
                if type_name not in valid_types:
                    self.logger.error(
                        f"Ошибка валидации типов: Колонка {field.name} имеет тип {type_name}, "
                        f"ожидался один из числовых типов: {valid_types}"
                    )
                    return False

        # 3. Проверка на наличие пустых (NULL) значений в ключевых колонках
        for col_name in expected_columns:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                self.logger.warning(
                    f"Предупреждение: Обнаружено {null_count} пустых (NULL) значений в колонке {col_name} "
                    f"перед отправкой INSERT."
                )

        self.logger.info("✅ Формат записи успешно верифицирован. Ошибок совместимости со схемой БД не обнаружено.")
        return True

    def seed_database(self, csv_path: str):
        """Читает CSV и заливает реальный датасет в Oracle (один раз)."""
        self.logger.info("Проверка наличия данных в Oracle...")
        try:
            df = self.extract_data(retries=1)
            if df.count() > 10:
                self.logger.info("Реальный датасет уже загружен в БД Oracle. Идем дальше.")
                return
        except (Py4JJavaError, AnalysisException) as e:
            # Конкретные ожидаемые ошибки: таблица еще не создана или пуста
            self.logger.info(
                f"Таблица не найдена или пуста ({type(e).__name__}). "
                f"Будет произведена первичная инициализация данных."
            )

        self.logger.info(f"Начинаем подготовку датасета {csv_path} для заливки в БД Oracle...")
        df_csv = self.spark.read.csv(
            csv_path, header=True, inferSchema=True, sep='\t', multiLine=True, escape='"'
        )

        df_to_db = df_csv.select(self.config.feature_columns).limit(50000)

        # Вызов валидатора формата записи
        if not self._verify_insert_format(df_to_db, self.config.feature_columns):
            raise ValueError("Исходные данные не прошли валидацию формата записи!")

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

        # Список колонок для результирующей таблицы (признаки + предсказанный кластер)
        expected_columns = self.config.feature_columns + ["cluster"]

        # Вызов валидатора формата записи перед INSERT
        if not self._verify_insert_format(df_to_write, expected_columns):
            raise ValueError("Результаты кластеризации не прошли валидацию формата записи!")

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