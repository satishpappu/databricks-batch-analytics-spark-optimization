from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "DatabricksBatchAnalytics") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    return spark