from dataclasses import dataclass, field
from typing import Dict


@dataclass
class SparkConfig:
    app_name: str = "Oracle_KMeans_Clustering"
    master: str = "local[*]"

    # Расширенная конфигурация Spark, настройки для JDBC
    configs: Dict[str, str] = field(default_factory=lambda: {
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        # драйвер, который скачали в Dockerfile
        "spark.jars": "/opt/spark/jars/ojdbc8.jar",
        "spark.driver.extraClassPath": "/opt/spark/jars/ojdbc8.jar"
    })