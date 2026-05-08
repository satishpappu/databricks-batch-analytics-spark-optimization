import argparse

from pyspark.sql import DataFrame

from src.utils.config_loader import load_config
from src.utils.spark_session import create_spark_session

def read_silver_delta(spark, input_path: str) -> DataFrame:
    return spark.read.format("delta").load(input_path)

def analyze_products(
    spark,
    dataset_name: str,
    silver_base_path: str,
) -> None:
    input_path = f"{silver_base_path}/{dataset_name}"

    print(f"Reading silver dataset: {input_path}")

    silver_df = read_silver_delta(spark, input_path)

    row_count = silver_df.count()
    print(f"{dataset_name} row count: {row_count}")

    partition_count = silver_df.rdd.getNumPartitions()
    print(f"{dataset_name} partition count: {partition_count}")

    product_category_df = silver_df.groupBy("category").count()
    print("Product count by category:")
    product_category_df.show(truncate=False)

    print("Execution plan:")
    product_category_df.explain(True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")

    args = parser.parse_args()

    config = load_config(args.config_path)

    silver_base_path = config["paths"]["silver"]

    spark = create_spark_session("SilverProductsProcessing")

    analyze_products(
        spark=spark,
        dataset_name="products",
        silver_base_path=silver_base_path,
    )

    spark.stop()