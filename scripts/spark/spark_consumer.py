import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window, sum as _sum, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

def main():
    spark = SparkSession.builder \
        .appName("CryptoWhaleTracker") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Connecting PySpark with Kafka...")

    # Read continuous data stream from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "crypto_trades") \
        .option("startingOffsets", "latest") \
        .load()

    schema = StructType([
        StructField("s", StringType(), True),
        StructField("m", BooleanType(), True),
        StructField("p", StringType(), True),
        StructField("q", StringType(), True),
        StructField("E", LongType(), True)
    ])

    # Cast Kafka data (Binary format) to String and Parse JSON
    parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select(
            col("data.s").alias("symbol"),
            col("data.m").alias("is_buyer_maker"),
            col("data.p").cast("double").alias("price"),
            col("data.q").cast("double").alias("quantity"),
            (col("data.p").cast("double")*col("data.q").cast("double")).alias("total_value"),
            col("data.E").alias("event_time")
        ) \
        .withColumn("timestamp", to_timestamp(col("event_time") / 1000).cast("timestamp")) \
        .withColumn("trade_type", when(col("is_buyer_maker"), "SELL").otherwise("BUY"))

    # Group by time window (1 minute, sliding every 10 seconds) + Coin Name + Trade Type
    aggregated_df = parsed_df \
        .groupBy(
            window(col("timestamp"), "1 minute", "10 seconds"),
            col("symbol"),
            col("trade_type")
        ) \
        .agg(_sum("total_value").alias("total_volume"))
    
    # Write results to PostgreSQL
    db_url = f"jdbc:postgresql://localhost:{os.environ.get('POSTGRES_PORT')}/crypto_streaming"
    db_properties = {
        "user": os.environ.get("POSTGRES_USER"),
        "password": os.environ.get("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }
    
    def write_to_postgres(batch_df, batch_id):
        # Split the window column into 2 start/end columns
        flat_batch_df = batch_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("trade_type"),
            col("total_volume")
        )

        flat_batch_df.write.jdbc(
            url=db_url,
            table="over5k_trade",
            mode="append",
            properties=db_properties
        )

    query = aggregated_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_postgres) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()