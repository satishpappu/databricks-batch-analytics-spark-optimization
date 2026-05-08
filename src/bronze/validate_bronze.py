import argparse

from pyspark.sql import DataFrame

from src.utils.config_loader import load_config
from src.utils.spark_session import create_spark_session


REQUIRED_AUDIT_COLUMNS = {
    "source_system",
    "batch_id",
    "ingestion_timestamp",
    "source_file",
}


def read_bronze_delta(spark, input_path: str) -> DataFrame:
    return spark.read.format("delta").load(input_path)


def validate_dataset(
    spark,
    dataset_name: str,
    bronze_base_path: str,
) -> None:
    input_path = f"{bronze_base_path}/{dataset_name}"

    print(f"\nReading Bronze dataset: {input_path}")

    bronze_df = read_bronze_delta(spark, input_path)

    print(f"\n{dataset_name} schema:")
    bronze_df.printSchema()

    row_count = bronze_df.count()
    print(f"{dataset_name} row count: {row_count}")

    missing_audit_columns = REQUIRED_AUDIT_COLUMNS - set(bronze_df.columns)

    if missing_audit_columns:
        raise ValueError(
            f"{dataset_name} is missing audit columns: {missing_audit_columns}"
        )

    print(f"{dataset_name} audit column validation passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")

    args = parser.parse_args()

    config = load_config(args.config_path)

    bronze_base_path = config["paths"]["bronze"]

    spark = create_spark_session("BronzeValidation")

    datasets = [
        "products",
        "customers",
        "orders",
        "order_items",
    ]

    for dataset_name in datasets:
        validate_dataset(
            spark=spark,
            dataset_name=dataset_name,
            bronze_base_path=bronze_base_path,
        )

    spark.stop()