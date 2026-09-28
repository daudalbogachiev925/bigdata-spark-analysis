"""Главный скрипт пайплайна."""
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.loader import create_spark_session, load_csv, load_config
from src.cleaner import clean_all
from src.aggregator import aggregate_by, top_products, monthly_dynamics, region_product_matrix
from src.exporter import save_to_csv, save_report

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def main():
    config = load_config("config.yaml")
    spark = create_spark_session(config)

    try:
        df = load_csv(spark, config["paths"]["input"])
        df_clean = clean_all(df, config)

        by_region = aggregate_by(df_clean, "region", config["aggregation"]["metrics"])
        by_month = monthly_dynamics(df_clean)
        top = top_products(df_clean, n=5)
        matrix = region_product_matrix(df_clean)

        print("\n=== По регионам ===")
        by_region.show()
        print("\n=== По месяцам ===")
        by_month.show()
        print("\n=== Топ-5 товаров ===")
        top.show()
        print("\n=== Регион × Товар ===")
        matrix.show()

        out = config["paths"]["output_dir"]
        save_to_csv(by_region, out, "by_region.csv")
        save_to_csv(by_month, out, "by_month.csv")
        save_to_csv(top, out, "top_products.csv")
        save_to_csv(matrix, out, "region_product.csv")

        stats = {
            "Всего записей": df.count(),
            "После очистки": df_clean.count(),
            "Регионов": by_region.count(),
            "Месяцев": by_month.count(),
        }
        save_report(stats, config["paths"]["report"])
        logger.info("Пайплайн завершён успешно")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
