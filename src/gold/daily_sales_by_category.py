import argparse

from pyspark.sql import DataFrame
from pyspark.sql.functions import sum, col, countDistinct, to_date
from src.utils.config_loader import load_config
from src.utils.spark_session import create_spark_session

def read_silver_delta(spark, input_path: str) -> DataFrame:
    return spark.read.format("delta").load(input_path)

def write_gold_delta(df: DataFrame, output_path: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(output_path)
    )

def daily_sales_by_category(
    spark,
    datasets: list,
    silver_base_path: str,
    gold_base_path: str,
) -> None:

    silver_dfs = {}

    for dataset_name in datasets:
        input_path = f"{silver_base_path}/{dataset_name}"
        print(f"Reading silver dataset: {input_path}")
        silver_dfs[dataset_name] = read_silver_delta(
            spark,
            input_path,
        )

    orders_df = silver_dfs["orders"]
    order_items_df = silver_dfs["order_items"]
    products_df = silver_dfs["products"]

    order_items_consolidated_df = orders_df.join(order_items_df, on="order_id", how="inner")
    order_items_products_df = order_items_consolidated_df.join(products_df, on="product_id", how="inner")

    order_items_products_df = order_items_products_df.withColumn("order_date",to_date(col("order_timestamp")))
    daily_sales_by_category_df = order_items_products_df \
                                  .groupBy(col("order_date"),col("category")) \
                                  .agg(sum(col("line_total")).alias("total_sales"),
                                       sum(col("quantity")).alias("total_items_sold"),
                                       countDistinct(col("order_id")).alias("total_orders")
                                  )

    daily_sales_by_category_df.explain(True)

    output_path = f"{gold_base_path}/daily_sales_by_category"
    print(f"Writing Gold Delta dataset: {output_path}")
    write_gold_delta(daily_sales_by_category_df, output_path)

    print(f"Completed Gold ingestion for: daily_sales_by_category")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-path", default="configs/dev.yaml")

    args = parser.parse_args()

    config = load_config(args.config_path)

    silver_base_path = config["paths"]["silver"]
    gold_base_path = config["paths"]["gold"]

    spark = create_spark_session("GoldDailySalesProcessing")

    daily_sales_by_category(
        spark=spark,
        datasets=["orders","order_items","products"],
        silver_base_path=silver_base_path,
        gold_base_path=gold_base_path,
    )

    spark.stop()