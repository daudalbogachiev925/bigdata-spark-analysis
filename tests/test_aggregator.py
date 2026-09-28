"""Тесты агрегаций."""
import pytest
from pyspark.sql import SparkSession
from src.aggregator import aggregate_by, top_products


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


@pytest.fixture
def sample_df(spark):
    data = [
        (1, "North", "2024-01", "Apple", 100),
        (2, "South", "2024-01", "Banana", 50),
        (3, "North", "2024-02", "Apple", 150),
        (4, "East", "2024-02", "Orange", 200),
    ]
    return spark.createDataFrame(data, ["order_id", "region", "month", "product", "amount"])


def test_aggregate_by_region(sample_df):
    result = aggregate_by(sample_df, "region", ["total_sales", "orders_count"])
    rows = {r["region"]: r for r in result.collect()}
    assert rows["North"]["total_sales"] == 250
    assert rows["North"]["orders_count"] == 2


def test_top_products(sample_df):
    result = top_products(sample_df, n=2)
    products = [r["product"] for r in result.collect()]
    assert products[0] == "Apple"
    assert len(products) == 2
