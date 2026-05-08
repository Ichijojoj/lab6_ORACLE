import logging
from pyspark.sql import SparkSession
from src.spark_config import SparkConfig


class SparkManager:
    def __init__(self, config: SparkConfig):
        self.config = config
        self._spark = None
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def spark(self) -> SparkSession:
        if self._spark is None:
            self.logger.info(f"Инициализация Spark сессии: {self.config.app_name}")


            builder = SparkSession.builder.appName(self.config.app_name).master(self.config.master)

            for key, value in self.config.configs.items():
                builder = builder.config(key, value)

            self._spark = builder.getOrCreate()
            self._spark.sparkContext.setLogLevel("WARN")  # INFO слишком шумный для JDBC
        return self._spark

    def stop(self) -> None:
        if self._spark:
            self.logger.info("Завершение Spark сессии.")
            self._spark.stop()