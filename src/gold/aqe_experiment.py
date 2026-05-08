import argparse
import time

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, countDistinct, sum, to_date

from src.utils.config_loader import load_config
from src.utils.spark_session import create_spark_session


def read_silver_delta(spark, input_path: str) -> DataFrame:
    return spark.read.format("delta").load(input_path)

def run_experiment(
    experiment_name: str,
    orders_df: DataFrame,
    order_items_df: DataFrame,
    products_df: DataFrame,
    aqe_flag: bool = False,
) -> None:
    spark = orders_df.sparkSession

    spark.conf.set(
        "spark.sql.adaptive.enabled",
        aqe_flag,
    )

    result_df = (
        orders_df
        .join(order_items_df, on="order_id", how="inner")
        .join(products_df, on="product_id", how="inner")
        .withColumn("order_date", to_date(col("order_timestamp")))
    ).groupBy("order_date", "category").agg(
        sum(col("line_total")).alias("total_sales"),
        sum(col("quantity")).alias("total_items_sold"),
        countDistinct(col("order_id")).alias("total_orders"),
    )

    start_time = time.time()
    result_df.collect()
    runtime_seconds = round(time.time() - start_time, 2)

    print("\n" + "=" * 80)
    print(f"Experiment: {experiment_name}")
    print(f"AQE enabled: {aqe_flag}")
    print(f"Runtime seconds: {runtime_seconds}")
    print("=" * 80)

    result_df.explain(True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")
    args = parser.parse_args()

    config = load_config(args.config_path)

    silver_base_path = config["paths"]["silver"]

    spark = create_spark_session("AQEExperiment")

    orders_df = read_silver_delta(spark, f"{silver_base_path}/orders")
    order_items_df = read_silver_delta(spark, f"{silver_base_path}/order_items")
    products_df = read_silver_delta(spark, f"{silver_base_path}/products")

    run_experiment(
        experiment_name="AQE disabled",
        orders_df=orders_df,
        order_items_df=order_items_df,
        products_df=products_df,
        aqe_flag=False,
    )

    run_experiment(
        experiment_name="AQE enabled",
        orders_df=orders_df,
        order_items_df=order_items_df,
        products_df=products_df,
        aqe_flag=True,
    )

    spark.stop()