"""Очистка данных: дубликаты, пропуски, выбросы."""
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, mean

logger = logging.getLogger(__name__)


def remove_duplicates(df: DataFrame, subset: list = None) -> DataFrame:
    """Удалить дубликаты."""
    before = df.count()
    df = df.dropDuplicates(subset)
    after = df.count()
    logger.info(f"Удалено дубликатов: {before - after}")
    return df


def fill_missing(df: DataFrame, strategy: str = "median") -> DataFrame:
    """Заполнить пропуски в числовых колонках."""
    numeric_cols = [f.name for f in df.schema.fields
                    if str(f.dataType) in ("IntegerType()", "DoubleType()")]

    for col_name in numeric_cols:
        if strategy == "median":
            median_val = df.approxQuantile(col_name, [0.5], 0.01)[0]
            df = df.fillna({col_name: median_val})
        elif strategy == "mean":
            mean_val = df.select(mean(col(col_name))).collect()[0][0]
            df = df.fillna({col_name: mean_val})
        elif strategy == "zero":
            df = df.fillna({col_name: 0})

    logger.info(f"Заполнены пропуски (стратегия: {strategy})")
    return df


def remove_outliers(df: DataFrame, multiplier: float = 1.5) -> DataFrame:
    """Удалить выбросы через IQR."""
    numeric_cols = [f.name for f in df.schema.fields
                    if str(f.dataType) in ("IntegerType()", "DoubleType()")]
    before = df.count()

    for col_name in numeric_cols:
        quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.01)
        Q1, Q3 = quantiles[0], quantiles[1]
        IQR = Q3 - Q1
        lower = Q1 - multiplier * IQR
        upper = Q3 + multiplier * IQR
        df = df.filter((col(col_name) >= lower) & (col(col_name) <= upper))

    after = df.count()
    logger.info(f"Удалено выбросов: {before - after}")
    return df


def clean_all(df: DataFrame, config: dict) -> DataFrame:
    """Полный пайплайн очистки."""
    if config["cleaning"]["drop_duplicates"]:
        df = remove_duplicates(df)
    if config["cleaning"]["fill_missing"]:
        df = fill_missing(df, config["cleaning"]["fill_missing"])
    if config["cleaning"]["remove_outliers"]:
        df = remove_outliers(df, config["cleaning"]["iqr_multiplier"])
    return df
