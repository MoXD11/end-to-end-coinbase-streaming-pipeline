from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from influxdb import InfluxDBClient
import time

spark = SparkSession.builder \
    .appName("WikiKafkaToInflux") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "512m") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "coindata") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "500") \
    .load()

from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import col, from_json, to_timestamp

schema = StructType([
    StructField("ask",        StringType(), True),
    StructField("bid",        StringType(), True),
    StructField("volume",     StringType(), True),
    StructField("trade_id",   LongType(),   True),
    StructField("price",      StringType(), True),
    StructField("size",       StringType(), True),
    StructField("time",       StringType(), True),
    StructField("rfq_volume", StringType(), True),
])

parsed_df = (
    df.selectExpr("CAST(value AS STRING) AS raw")
        .select(from_json(col("raw"), schema).alias("d"))
        .select("d.*")
        .filter(col("price").isNotNull() & col("trade_id").isNotNull())
        .withColumn("ask",        col("ask").cast("double"))
        .withColumn("bid",        col("bid").cast("double"))
        .withColumn("volume",     col("volume").cast("double"))
        .withColumn("price",      col("price").cast("double"))
        .withColumn("size",       col("size").cast("double"))
        .withColumn("rfq_volume", col("rfq_volume").cast("double"))
        .withColumn("time",       to_timestamp(col("time")))
        .fillna({"rfq_volume": 0.0})
)

from pyspark.sql.functions import col, lit, concat_ws, lpad

MAX_TS = 9999999999999  # 13 digits, milliseconds

keyed_df = (
    parsed_df
    .withColumn("product_id", lit("BTC-USD"))
    .withColumn("epoch_ms", (col("time").cast("double") * 1000).cast("long"))
    .withColumn(
        "rowkey",
        concat_ws("#",
                    col("product_id"),
                    lpad((lit(MAX_TS) - col("epoch_ms")).cast("string"), 13, "0"),
                    col("trade_id").cast("string")))
)

import happybase

COLUMNS = ["ask", "bid", "volume", "trade_id", "price", "size", "time", "rfq_volume"]

def write_partition(rows):
    conn = happybase.Connection("localhost", port=9090, timeout=30000)
    table = conn.table("ticker")
    with table.batch(batch_size=1000) as batch:
        for r in rows:
            data = {}
            for c in COLUMNS:
                v = r[c]
                if v is not None:
                    data[f"d:{c}".encode()] = str(v).encode()
            batch.put(r["rowkey"].encode(), data)
    conn.close()

def write_to_hbase(batch_df, batch_id):
    batch_df.select("rowkey", *COLUMNS).foreachPartition(write_partition)

query = (
    keyed_df.writeStream
        .foreachBatch(write_to_hbase)
        .option("checkpointLocation", "/tmp/checkpoints/ticker_hbase")
        .outputMode("append")
        .start()
)

query = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_all) \
    .option("checkpointLocation", "/home/bigdata/checkpoints/coindata") \
    .trigger(processingTime="15 seconds") \
    .start()

query.awaitTermination()