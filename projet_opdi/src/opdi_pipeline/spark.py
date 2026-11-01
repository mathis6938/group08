"""SparkSession of the pipeline (same settings as in the labs, plus UTC as session time zone)."""
from pyspark.sql import SparkSession


def get_spark(settings):
    spark = (SparkSession.builder
             .appName(settings.app_name)
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.shuffle.partitions", str(settings.shuffle_partitions))
             .config("spark.driver.memory", settings.driver_memory)
             .config("spark.sql.session.timeZone", "UTC")          # all timestamps of the pipeline are UTC
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    # Bronze: a re-run only replaces the partitions (source periods) it writes
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark
