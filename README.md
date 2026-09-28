# bigdata-spark-analysis# Big Data: Spark Analysis

Полный пайплайн анализа больших данных на PySpark.

## Задачи
1. Загрузка данных из CSV
2. Очистка: пропуски, дубликаты, выбросы
3. Агрегация: по регионам, месяцам, товарам
4. Аналитика: топ-товары, динамика, воронка
5. Экспорт результатов
6. Тестирование

## Стек
- Python 3.11
- PySpark 3.5
- pandas, matplotlib
- pytest

## Структура
- `src/loader.py` — загрузка данных
- `src/cleaner.py` — очистка
- `src/aggregator.py` — агрегации
- `src/exporter.py` — экспорт
- `scripts/run_pipeline.py` — запуск пайплайна
- `tests/` — тесты

## Запуск
```bash
pip install -r requirements.txt
python scripts/run_pipeline.py
