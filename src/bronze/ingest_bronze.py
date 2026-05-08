import argparse
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, lit

from src.utils.config_loader import load_config
from src.utils.spark_session import create_spark_session


def read_raw_csv(spark, input_path: str) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )


def add_bronze_audit_columns(df: DataFrame, source_name: str, batch_id: str) -> DataFrame:
    return (
        df
        .withColumn("source_system", lit(source_name))
        .withColumn("batch_id", lit(batch_id))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )


def write_bronze_delta(df: DataFrame, output_path: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(output_path)
    )


def ingest_dataset(
    spark,
    dataset_name: str,
    raw_base_path: str,
    bronze_base_path: str,
    batch_id: str,
) -> None:
    input_path = f"{raw_base_path}/{dataset_name}/{dataset_name}.csv"
    output_path = f"{bronze_base_path}/{dataset_name}"

    print(f"Reading raw dataset: {input_path}")

    raw_df = read_raw_csv(spark, input_path)
    bronze_df = add_bronze_audit_columns(
        df=raw_df,
        source_name=dataset_name,
        batch_id=batch_id,
    )

    print(f"Writing Bronze Delta dataset: {output_path}")
    write_bronze_delta(bronze_df, output_path)

    print(f"Completed Bronze ingestion for: {dataset_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")
    parser.add_argument("--batch-id", default="manual_batch_001")

    args = parser.parse_args()

    config = load_config(args.config_path)

    raw_base_path = config["paths"]["raw_data"]
    bronze_base_path = config["paths"]["bronze"]

    spark = create_spark_session("BronzeIngestion")

    datasets = [
        "products",
        "customers",
        "orders",
        "order_items",
    ]

    for dataset_name in datasets:
        ingest_dataset(
            spark=spark,
            dataset_name=dataset_name,
            raw_base_path=raw_base_path,
            bronze_base_path=bronze_base_path,
            batch_id=args.batch_id,
        )

    spark.stop()