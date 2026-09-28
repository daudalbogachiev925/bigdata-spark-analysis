"""Агрегации данных."""
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, avg, count, desc

logger = logging.getLogger(__name__)


def aggregate_by(df: DataFrame, group_col: str, metrics: list) -> DataFrame:
    """Агрегировать по колонке с метриками."""
    agg_exprs = []
    for m in metrics:
        if m == "total_sales":
            agg_exprs.append(sum("amount").alias("total_sales"))
        elif m == "avg_sale":
            agg_exprs.append(avg("amount").alias("avg_sale"))
        elif m == "orders_count":
            agg_exprs.append(count("order_id").alias("orders_count"))

    result = df.groupBy(group_col).agg(*agg_exprs).orderBy(desc("total_sales"))
    logger.info(f"Агрегация по {group_col}: {result.count()} групп")
    return result


def top_products(df: DataFrame, n: int = 10) -> DataFrame:
    """Топ-N товаров по выручке."""
    result = df.groupBy("product") \
        .agg(sum("amount").alias("revenue"), count("order_id").alias("orders")) \
        .orderBy(desc("revenue")) \
        .limit(n)
    logger.info(f"Топ-{n} товаров")
    return result


def monthly_dynamics(df: DataFrame) -> DataFrame:
    """Динамика по месяцам."""
    result = df.groupBy("month") \
        .agg(sum("amount").alias("total"), count("order_id").alias("orders")) \
        .orderBy("month")
    logger.info(f"Динамика по месяцам: {result.count()} месяцев")
    return result


def region_product_matrix(df: DataFrame) -> DataFrame:
    """Матрица регион × товар."""
    result = df.groupBy("region", "product") \
        .agg(sum("amount").alias("total")) \
        .orderBy("region", "product")
    return result
