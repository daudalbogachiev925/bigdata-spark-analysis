"""Экспорт результатов."""
import logging
import os
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def save_to_csv(df: DataFrame, path: str, filename: str):
    """Сохранить DataFrame в CSV (через pandas)."""
    os.makedirs(path, exist_ok=True)
    pandas_df = df.toPandas()
    full_path = os.path.join(path, filename)
    pandas_df.to_csv(full_path, index=False, encoding="utf-8")
    logger.info(f"Сохранено: {full_path} ({len(pandas_df)} строк)")


def save_report(stats: dict, path: str):
    """Сохранить текстовый отчёт."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("=== ОТЧЁТ ===\n\n")
        for key, value in stats.items():
            f.write(f"{key}: {value}\n")
    logger.info(f"Отчёт сохранён: {path}")
