import argparse
import time

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, countDistinct, sum, to_date

from src.utils.config_loader import load_config
from src.utils.spark_session import create_spark_session


def read_silver_delta(spark, input_path: str) -> DataFrame:
    return spark.read.format("delta").load(input_path)


def build_joined_df(
    orders_df: DataFrame,
    order_items_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    return (
        orders_df
        .join(order_items_df, on="order_id", how="inner")
        .join(products_df, on="product_id", how="inner")
        .withColumn("order_date", to_date(col("order_timestamp")))
    )


def run_experiment(
    experiment_name: str,
    orders_df: DataFrame,
    order_items_df: DataFrame,
    products_df: DataFrame,
    cache_flag: bool,
) -> None:
    joined_df = build_joined_df(
        orders_df=orders_df,
        order_items_df=order_items_df,
        products_df=products_df,
    )

    if cache_flag:
        joined_df.cache()
        joined_df.count()  # materialize cache

    start_time = time.time()

    row_count = joined_df.count()

    daily_sales_df = (
        joined_df
        .groupBy("order_date", "category")
        .agg(
            sum(col("line_total")).alias("total_sales"),
            sum(col("quantity")).alias("total_items_sold"),
            countDistinct(col("order_id")).alias("total_orders"),
        )
    )
    daily_sales_df.collect()

    electronics_count = joined_df.filter(col("category") == "electronics").count()

    runtime_seconds = round(time.time() - start_time, 2)

    print("\n" + "=" * 80)
    print(f"Experiment: {experiment_name}")
    print(f"Caching enabled: {cache_flag}")
    print(f"Row count: {row_count}")
    print(f"Electronics row count: {electronics_count}")
    print(f"Runtime seconds: {runtime_seconds}")
    print("=" * 80)

    daily_sales_df.explain(True)

    if cache_flag:
        joined_df.unpersist()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")
    args = parser.parse_args()

    config = load_config(args.config_path)

    silver_base_path = config["paths"]["silver"]

    spark = create_spark_session("CacheExperiment")

    orders_df = read_silver_delta(spark, f"{silver_base_path}/orders")
    order_items_df = read_silver_delta(spark, f"{silver_base_path}/order_items")
    products_df = read_silver_delta(spark, f"{silver_base_path}/products")

    run_experiment(
        experiment_name="Caching disabled",
        orders_df=orders_df,
        order_items_df=order_items_df,
        products_df=products_df,
        cache_flag=False,
    )

    run_experiment(
        experiment_name="Caching enabled",
        orders_df=orders_df,
        order_items_df=order_items_df,
        products_df=products_df,
        cache_flag=True,
    )

    spark.stop()