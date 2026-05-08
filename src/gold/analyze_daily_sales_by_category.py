import argparse

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum
from src.utils.config_loader import load_config
from src.utils.spark_session import create_spark_session

def read_gold_delta(spark, input_path: str) -> DataFrame:
    return spark.read.format("delta").load(input_path)

def daily_sales_analysis(
    spark,
    dataset_name: str,
    gold_base_path: str,
) -> None:

    input_path = f"{gold_base_path}/{dataset_name}"
    print(f"Reading gold dataset: {input_path}")
    gold_df = read_gold_delta(spark, input_path)

    row_count = gold_df.count()
    print(f"{dataset_name} row count: {row_count}")

    partition_count = gold_df.rdd.getNumPartitions()
    print(f"{dataset_name} partition count: {partition_count}")

    print("Top 20 rows:")
    gold_df.show(20, truncate=False)

    product_category_df = (
        gold_df
        .groupBy("category")
        .agg(sum(col("total_sales")).alias("category_total_sales"))
    )

    product_category_df.show(20, truncate=False)
    product_category_df.explain(True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")

    args = parser.parse_args()

    config = load_config(args.config_path)

    gold_base_path = config["paths"]["gold"]

    spark = create_spark_session("DailySalesAnalysis")

    daily_sales_analysis(
        spark=spark,
        dataset_name="daily_sales_by_category",
        gold_base_path=gold_base_path,
    )

    spark.stop()