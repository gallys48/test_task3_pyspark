import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from pyspark.sql import SparkSession
from main import get_products_with_categories

# Создаем spark-сессию
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("test-task3") \
        .master("local[*]") \
        .getOrCreate()

# Проверка на правильный результат соединения
def test_products_with_and_without_categories(spark):
    #Создаем DataFrame продуктов
    products = spark.createDataFrame([
        (1, "Молоко"),
        (2, "Хлеб"),
        (3, "Сыр"),
    ], ["product_id", "product_name"])

    #Создаем DataFrame категорий
    categories = spark.createDataFrame([
        (10, "Молочные продукты"),
        (20, "Выпечка"),
    ], ["category_id", "category_name"])

    #Создаем DataFrame таблицы многие ко многим
    product_categories = spark.createDataFrame([
        (1, 10),  # Молоко -> Молочные
        (2, 20),  # Хлеб -> Выпечка
    ], ["product_id", "category_id"])

    result = get_products_with_categories(spark, products, categories, product_categories)

    data = [(row.product_name, row.category_name) for row in result.collect()]

    assert ("Молоко", "Молочные продукты") in data
    assert ("Хлеб", "Выпечка") in data
    assert ("Сыр", None) in data  # у "Сыр" нет категорий

# Проверка на дубликаты    
def test_no_duplicate_rows(spark):
    products = spark.createDataFrame([
        (1, "Молоко"),
        (1, "Молоко"),
    ], ["product_id", "product_name"])

    categories = spark.createDataFrame([
        (10, "Молочные продукты"),
        (10, "Молочные продукты"),
    ], ["category_id", "category_name"])

    product_categories = spark.createDataFrame([
        (1, 10),
        (1, 10),
    ], ["product_id", "category_id"])

    result = get_products_with_categories(spark, products, categories, product_categories)

    data = [(row.product_name, row.category_name) for row in result.collect()]

    # Проверяем, что после удаления дубликатов остается ровно 1 уникальная пара
    assert len(data) == 1
    assert ("Молоко", "Молочные продукты") in data