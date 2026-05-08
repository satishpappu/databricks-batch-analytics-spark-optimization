import argparse

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, trim, lower

from src.utils.config_loader import load_config
from src.utils.spark_session import create_spark_session


def read_bronze_delta(spark, input_path: str) -> DataFrame:
    return spark.read.format("delta").load(input_path)


def transform_products(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["product_id"])
        .withColumn("category", lower(trim(df["category"])))
        .withColumn("product_name", trim(df["product_name"]))
        .withColumn("brand", trim(df["brand"]))
        .filter(df["unit_price"] > 0)
        .withColumn("silver_processing_timestamp", current_timestamp())
    )

def write_silver_delta(df: DataFrame, output_path: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(output_path)
    )


def process_products(
    spark,
    dataset_name: str,
    bronze_base_path: str,
    silver_base_path: str,
) -> None:
    input_path = f"{bronze_base_path}/{dataset_name}"
    output_path = f"{silver_base_path}/{dataset_name}"

    print(f"Reading bronze dataset: {input_path}")

    bronze_df = read_bronze_delta(spark, input_path)
    silver_df = transform_products(bronze_df)

    print(f"Writing Silver Delta dataset: {output_path}")
    write_silver_delta(silver_df, output_path)

    print(f"Completed Silver ingestion for: {dataset_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")

    args = parser.parse_args()

    config = load_config(args.config_path)

    bronze_base_path = config["paths"]["bronze"]
    silver_base_path = config["paths"]["silver"]

    spark = create_spark_session("SilverProductsProcessing")

    process_products(
        spark=spark,
        dataset_name="products",
        bronze_base_path=bronze_base_path,
        silver_base_path=silver_base_path,
    )

    spark.stop()