import argparse
import time

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum

from src.utils.config_loader import load_config
from src.utils.spark_session import create_spark_session


def read_gold_delta(spark, input_path: str) -> DataFrame:
    return spark.read.format("delta").load(input_path)


def run_experiment(gold_df: DataFrame, shuffle_partitions: int) -> None:
    gold_df.sparkSession.conf.set(
        "spark.sql.shuffle.partitions",
        str(shuffle_partitions),
    )

    result_df = (
        gold_df
        .groupBy("category")
        .agg(sum(col("total_sales")).alias("category_total_sales"))
    )

    start_time = time.time()
    result_df.collect()
    end_time = time.time()

    print(f"Shuffle partitions: {shuffle_partitions}")
    print(f"Runtime seconds: {round(end_time - start_time, 2)}")
    result_df.explain(True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")
    args = parser.parse_args()

    config = load_config(args.config_path)

    gold_base_path = config["paths"]["gold"]
    input_path = f"{gold_base_path}/daily_sales_by_category"

    spark = create_spark_session("ShufflePartitionExperiment")

    gold_df = read_gold_delta(spark, input_path)

    run_experiment(gold_df, 200)
    run_experiment(gold_df, 8)

    spark.stop()