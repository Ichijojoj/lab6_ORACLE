import os
from dataclasses import dataclass, field
from typing import List

@dataclass
class AppConfig:
    # Настройки ML
    model_save_path: str = "models/kmeans_food_model"
    feature_columns: List[str] = field(default_factory=lambda: [
        'energy_100g', 'fat_100g', 'carbohydrates_100g', 'sugars_100g', 'proteins_100g', 'salt_100g'
    ])
    k_clusters: int = 5
    random_seed: int = 42
    max_iter: int = 20

    # Настройки Oracle DB (читаются из переменных окружения Docker)
    db_url: str = os.getenv("DB_URL", "jdbc:oracle:thin:@localhost:1521/XE")
    db_user: str = os.getenv("DB_USER", "SYSTEM")
    db_password: str = os.getenv("DB_PASSWORD", "oracle_password")
    db_driver: str = "oracle.jdbc.driver.OracleDriver"

    # Таблицы протокола взаимодействия
    source_table: str = "RAW_FOOD_DATA"
    target_table: str = "FOOD_CLUSTERS_RESULT"