import argparse

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, floor, lit, rand, when

from src.utils.config_loader import load_config
from src.utils.spark_session import create_spark_session


def read_silver_delta(spark, input_path: str) -> DataFrame:
    return spark.read.format("delta").load(input_path)


def analyze_partition_distribution(df: DataFrame, label: str) -> None:
    partition_sizes = df.rdd.glom().map(len).collect()

    print(f"\n{label}")
    print(f"Partition row counts: {partition_sizes}")
    print(f"Number of partitions: {len(partition_sizes)}")
    print(f"Max partition size: {max(partition_sizes)}")
    print(f"Min partition size: {min(partition_sizes)}")


def run_experiment(
    order_items_df: DataFrame,
    products_df: DataFrame,
    salt_buckets: int,
) -> None:
    joined_df = order_items_df.join(
        products_df,
        on="product_id",
        how="inner",
    )

    print("\nCategory distribution:")
    joined_df.groupBy("category").count().orderBy(col("count").desc()).show(
        truncate=False
    )

    unsalted_df = joined_df.repartition("category")
    analyze_partition_distribution(
        df=unsalted_df,
        label="Before salting: repartition by category",
    )

    salted_df = joined_df.withColumn(
        "salted_category",
        when(
            col("category") == "electronics",
            concat(
                col("category"),
                lit("_"),
                floor(rand() * salt_buckets).cast("int"),
            ),
        ).otherwise(col("category")),
    )

    print("\nSalted category distribution:")
    salted_df.groupBy("salted_category").count().orderBy(
        col("count").desc()
    ).show(truncate=False)

    salted_repartitioned_df = salted_df.repartition("salted_category")
    analyze_partition_distribution(
        df=salted_repartitioned_df,
        label="After salting: repartition by salted_category",
    )

    print("\nExecution plan after salting:")
    salted_repartitioned_df.groupBy("salted_category").count().explain(True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")
    parser.add_argument("--salt-buckets", type=int, default=5)

    args = parser.parse_args()

    config = load_config(args.config_path)
    silver_base_path = config["paths"]["silver"]

    spark = create_spark_session("SaltingExperiment")

    order_items_df = read_silver_delta(spark, f"{silver_base_path}/order_items")
    products_df = read_silver_delta(spark, f"{silver_base_path}/products")

    run_experiment(
        order_items_df=order_items_df,
        products_df=products_df,
        salt_buckets=args.salt_buckets,
    )

    spark.stop()