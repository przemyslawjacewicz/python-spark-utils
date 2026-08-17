import logging
from datetime import datetime

import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from python_databricks_env import dbutils

logger = logging.getLogger(__name__)


# todo: add tests
def get_version(spark: SparkSession, path: str, at: datetime | None = None) -> int | None:
    if DeltaTable.isDeltaTable(spark, path):
        history = DeltaTable.forPath(spark, path).history()
        if at is None:
            return history.select("version").orderBy(F.col("timestamp").desc()).collect()[0][0]
        else:
            df = history.where(F.col("timestamp") <= at)
            if df.isEmpty():
                return None
            else:
                return df.select("version").orderBy(F.col("timestamp").desc()).collect()[0][0]
    else:
        return None


# todo: consider returning something 
# todo: add tests
def restore_to_version(spark: SparkSession, path: str, version: int | None) -> None:
    if version is None:
        dbutils.fs.rm(path, True)
    else:
        DeltaTable.forPath(spark, path).restoreToVersion(version)


# todo: consider returning something
# todo: add tests
def restore_at(spark: SparkSession, path: str, at: datetime | None) -> None:
    if at is None:
        dbutils.fs.rm(path, True)
    else:
        restore_to_version(spark, path, get_version(spark, path, at))


# todo: add tests
def get_diff_by_col(spark: SparkSession, path: str, base_version: int, version: int | None, col: str) -> DataFrame:
    df_base = spark.read.format("delta").option("versionAsOf", base_version).load(path)
    col_value_base = df_base.select(F.max(col)).collect()[0][0]
    condition = F.col(col) > col_value_base

    if version is None:
        df = spark.read.format("delta").load(path)
    else:
        df = spark.read.format("delta").option("versionAsOf", version).load(path)

    return df.where(condition)


# todo: add tests
def get_diff_by_ts(spark: SparkSession, path: str, base_version: int, version: int | None, col: str) -> DataFrame:
    col_value_base = (
        DeltaTable.forPath(spark, path).history()
        .where(F.col("version") == base_version)
        .select("timestamp")
        .collect()[0][0]
    )
    condition = F.col(col) > col_value_base

    if version is None:
        df = spark.read.format("delta").load(path)
    else:
        df = spark.read.format("delta").option("versionAsOf", version).load(path)

    return df.where(condition)


# todo: add tests
def get_diff_by_cdf(spark: SparkSession, path: str, starting_version: int, ending_version: int | None) -> DataFrame:
    reader = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", starting_version)
    )

    if ending_version is None:
        changed = reader.load(path)
    else:
        changed = reader.option("endingVersion", ending_version).load(path)

    return (
        changed
        .where(F.col("_change_type").isin("insert", "update_postimage", "delete"))
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )
