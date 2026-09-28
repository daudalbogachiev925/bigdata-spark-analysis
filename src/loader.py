
"""Загрузка данных из CSV в Spark DataFrame."""
import logging
from pyspark.sql import SparkSession, DataFrame
import yaml

logger = logging.getLogger(__name__)


def create_spark_session(config: dict) -> SparkSession:
    """Создать Spark-сессию с настройками из конфига."""
    spark = SparkSession.builder \
        .appName(config["spark"]["app_name"]) \
        .config("spark.sql.shuffle.partitions", config["spark"]["shuffle_partitions"]) \
        .getOrCreate()
    spark.sparkContext.setLogLevel(config["spark"]["log_level"])
    logger.info(f"Spark session создана: {config['spark']['app_name']}")
    return spark


def load_csv(spark: SparkSession, path: str) -> DataFrame:
    """Загрузить CSV в DataFrame."""
    logger.info(f"Загрузка данных из {path}")
    df = spark.read.csv(path, header=True, inferSchema=True)
    logger.info(f"Загружено строк: {df.count()}")
    return df


def load_config(path: str = "config.yaml") -> dict:
    """Загрузить YAML-конфиг."""
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)
