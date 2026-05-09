import argparse
import time

from pyspark.sql import DataFrame

from src.utils.config_loader import load_config
from src.utils.spark_session import create_spark_session


def read_gold_delta(spark, input_path: str) -> DataFrame:
    return spark.read.format("delta").load(input_path)


def analyze_partitions(df: DataFrame, label: str) -> None:
    partition_sizes = df.rdd.glom().map(len).collect()

    print("\n" + "=" * 80)
    print(label)
    print(f"Partition count: {len(partition_sizes)}")
    print(f"Partition sizes: {partition_sizes}")
    print(f"Max partition size: {max(partition_sizes)}")
    print(f"Min partition size: {min(partition_sizes)}")
    print("=" * 80)


def run_repartition_experiment(df: DataFrame) -> None:
    start_time = time.time()

    repartitioned_df = df.repartition(5)

    repartitioned_df.collect()

    runtime_seconds = round(time.time() - start_time, 2)

    analyze_partitions(
        repartitioned_df,
        "Repartition(5) Results",
    )

    print(f"Runtime seconds: {runtime_seconds}")

    print("\nExecution plan for repartition:")
    repartitioned_df.explain(True)


def run_coalesce_experiment(df: DataFrame) -> None:
    start_time = time.time()

    coalesced_df = df.coalesce(5)

    coalesced_df.collect()

    runtime_seconds = round(time.time() - start_time, 2)

    analyze_partitions(
        coalesced_df,
        "Coalesce(5) Results",
    )

    print(f"Runtime seconds: {runtime_seconds}")

    print("\nExecution plan for coalesce:")
    coalesced_df.explain(True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")
    args = parser.parse_args()

    config = load_config(args.config_path)

    gold_base_path = config["paths"]["gold"]

    spark = create_spark_session("RepartitionVsCoalesce")

    input_path = f"{gold_base_path}/daily_sales_by_category"

    gold_df = read_gold_delta(spark, input_path)

    print(f"Original partition count: {gold_df.rdd.getNumPartitions()}")

    expanded_df = gold_df.repartition(50)

    print(f"Expanded partition count: {expanded_df.rdd.getNumPartitions()}")

    run_repartition_experiment(expanded_df)

    run_coalesce_experiment(expanded_df)

    spark.stop()