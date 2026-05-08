import argparse
import time

from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, col, countDistinct, sum, to_date

from src.utils.config_loader import load_config
from src.utils.spark_session import create_spark_session


def read_silver_delta(spark, input_path: str) -> DataFrame:
    return spark.read.format("delta").load(input_path)


def build_daily_sales_query(
    orders_df: DataFrame,
    order_items_df: DataFrame,
    products_df: DataFrame,
    use_explicit_broadcast: bool = False,
) -> DataFrame:
    products_to_join = broadcast(products_df) if use_explicit_broadcast else products_df

    joined_df = (
        orders_df
        .join(order_items_df, on="order_id", how="inner")
        .join(products_to_join, on="product_id", how="inner")
        .withColumn("order_date", to_date(col("order_timestamp")))
    )

    return (
        joined_df
        .groupBy("order_date", "category")
        .agg(
            sum(col("line_total")).alias("total_sales"),
            sum(col("quantity")).alias("total_items_sold"),
            countDistinct(col("order_id")).alias("total_orders"),
        )
    )


def run_experiment(
    experiment_name: str,
    orders_df: DataFrame,
    order_items_df: DataFrame,
    products_df: DataFrame,
    auto_broadcast_threshold: int,
    use_explicit_broadcast: bool = False,
) -> None:
    spark = orders_df.sparkSession

    spark.conf.set(
        "spark.sql.autoBroadcastJoinThreshold",
        auto_broadcast_threshold,
    )

    result_df = build_daily_sales_query(
        orders_df=orders_df,
        order_items_df=order_items_df,
        products_df=products_df,
        use_explicit_broadcast=use_explicit_broadcast,
    )

    start_time = time.time()
    result_df.collect()
    runtime_seconds = round(time.time() - start_time, 2)

    print("\n" + "=" * 80)
    print(f"Experiment: {experiment_name}")
    print(f"autoBroadcastJoinThreshold: {auto_broadcast_threshold}")
    print(f"Explicit broadcast hint: {use_explicit_broadcast}")
    print(f"Runtime seconds: {runtime_seconds}")
    print("=" * 80)

    result_df.explain(True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")
    args = parser.parse_args()

    config = load_config(args.config_path)

    silver_base_path = config["paths"]["silver"]

    spark = create_spark_session("BroadcastJoinExperiment")

    orders_df = read_silver_delta(spark, f"{silver_base_path}/orders")
    order_items_df = read_silver_delta(spark, f"{silver_base_path}/order_items")
    products_df = read_silver_delta(spark, f"{silver_base_path}/products")

    run_experiment(
        experiment_name="Broadcast disabled",
        orders_df=orders_df,
        order_items_df=order_items_df,
        products_df=products_df,
        auto_broadcast_threshold=-1,
        use_explicit_broadcast=False,
    )

    run_experiment(
        experiment_name="Automatic broadcast enabled",
        orders_df=orders_df,
        order_items_df=order_items_df,
        products_df=products_df,
        auto_broadcast_threshold=10485760,
        use_explicit_broadcast=False,
    )

    run_experiment(
        experiment_name="Explicit broadcast products",
        orders_df=orders_df,
        order_items_df=order_items_df,
        products_df=products_df,
        auto_broadcast_threshold=-1,
        use_explicit_broadcast=True,
    )

    spark.stop()