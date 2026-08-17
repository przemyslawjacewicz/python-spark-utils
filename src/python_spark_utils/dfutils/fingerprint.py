# todo: check how fingerprinting behaves when we have all null values
# todo: compare fingerprints for two dataframes where the second one's schema extends the first one's but all cols are nulls
# for fingerprinting check how this behaves when dataframes have different schema with null values in extra columns

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column


# todo: add tests
def fingerprint_col(cols: list, chunk_size: int = 100) -> Column:
    cols_sorted = sorted(cols)
    cols_chunk_hashed = [F.xxhash64(*cols_sorted[i:i + chunk_size]) for i in range(0, len(cols_sorted), chunk_size)]
    return F.xxhash64(*cols_chunk_hashed) if len(cols_chunk_hashed) > 1 else cols_chunk_hashed[0]


# todo: add tests
def fingerprint_dataframe(df: DataFrame, chunk_size: int = 100, buckets: int = 8) -> DataFrame:
    return (
        df
        .select(fingerprint_col(df.columns, chunk_size).alias("_row_hash"))
        .withColumn("_bucket", F.pmod(F.col("_row_hash"), F.lit(buckets)))
        .groupBy("_bucket", "_row_hash")
        .agg(F.count(F.lit(1)).alias("_row_count"))
    )
