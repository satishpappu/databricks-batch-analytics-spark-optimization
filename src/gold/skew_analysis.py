import argparse

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.utils.config_loader import load_config
from src.utils.spark_session import create_spark_session


def read_silver_delta(spark, input_path: str) -> DataFrame:
    return spark.read.format("delta").load(input_path)


def run_experiment(
    order_items_df: DataFrame,
    products_df: DataFrame,
) -> None:

    joined_df = order_items_df.join(
        products_df,
        on="product_id",
        how="inner",
    )

    print("Category distribution:")
    joined_df.groupBy("category").count().orderBy(col("count").desc()).show(
        truncate=False
    )

    repartitioned_df = joined_df.repartition("category")

    partition_sizes = repartitioned_df.rdd.glom().map(len).collect()

    print("Partition row counts after repartition by category:")
    print(partition_sizes)

    print(f"Number of partitions: {len(partition_sizes)}")
    print(f"Max partition size: {max(partition_sizes)}")
    print(f"Min partition size: {min(partition_sizes)}")

    repartitioned_df.groupBy("category").count().explain(True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")
    args = parser.parse_args()

    config = load_config(args.config_path)

    silver_base_path = config["paths"]["silver"]

    spark = create_spark_session("SkewAnalysis")

    order_items_df = read_silver_delta(spark, f"{silver_base_path}/order_items")
    products_df = read_silver_delta(spark, f"{silver_base_path}/products")

    run_experiment(
        order_items_df=order_items_df,
        products_df=products_df,
    )

    spark.stop()