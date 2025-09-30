from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

def get_products_with_categories(
    spark: SparkSession,
    products: DataFrame,
    categories: DataFrame,
    product_categories: DataFrame
) -> DataFrame:

    # Соединяем таблицу продуктов с таблице продуктов-категорий (многие ко многим)
    prod_with_links = products.join(
        product_categories,
        on="product_id",
        how="left"
    )

    # Делаем left-join таблицы категорий и выбираем нужные столбцы
    result = prod_with_links.join(
        categories,
        on="category_id",
        how="left"
    ).select(
        col("product_name"),
        col("category_name")
    )

    return result