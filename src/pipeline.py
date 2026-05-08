import logging
from py4j.protocol import Py4JJavaError
from pyspark.errors import AnalysisException, IllegalArgumentException

from src.config import AppConfig
from src.spark_config import SparkConfig
from src.spark_manager import SparkManager
from src.preprocessor import DataPreprocessor
from src.clustering import ClusteringModeler
from src.oracle_manager import OracleManager


class MLPipeline:
    """Оркестратор процесса машинного обучения с Oracle интеграцией."""

    def __init__(self):
        self.config = AppConfig()
        self.spark_config = SparkConfig()
        self.spark_manager = SparkManager(self.spark_config)
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self):
        spark = self.spark_manager.spark
        db_manager = OracleManager(spark, self.config)

        try:
            # 0. Инициализация (заливаем датасет в Oracle, если БД пустая)
            db_manager.seed_database("data/data.csv")

            # 1. Выгрузка данных из БД (Extract)
            raw_df = db_manager.extract_data()

            # 2. Обработка (Transform)
            preprocessor = DataPreprocessor(spark, self.config)
            clean_df = preprocessor.clean_data(raw_df)
            clean_df.cache()

            feature_pipeline = preprocessor.build_feature_pipeline()
            feature_model = feature_pipeline.fit(clean_df)
            ml_df = feature_model.transform(clean_df)

            #кластеризация
            modeler = ClusteringModeler(self.config)
            modeler.train(ml_df)
            modeler.evaluate(ml_df)
            modeler.save_model()

            # Получение DataFrame с предсказаниями
            predictions_df = modeler.model.transform(ml_df)

            #Загрузка результатов
            db_manager.load_results(predictions_df)

            self.logger.info("✅ Пайплайн успешно завершен!")

        #  конкретные бизнес-сценарии сбоев
        except Py4JJavaError as db_err:
            self.logger.critical("Сбой на уровне Java/JDBC взаимодействия (БД недоступна или драйвер не найден).")
            self.logger.error(str(db_err))
        except AnalysisException as sql_err:
            self.logger.critical("Ошибка обработки DataFrame/SQL (отсутствуют колонки или неверный синтаксис).")
            self.logger.error(str(sql_err))
        except IllegalArgumentException as arg_err:
            self.logger.critical("Неверные параметры для модели кластеризации.")
            self.logger.error(str(arg_err))
        except ConnectionError as conn_err:
            self.logger.critical(str(conn_err))
        finally:
            self.spark_manager.stop()