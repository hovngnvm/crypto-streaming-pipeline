import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as _sum, when
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
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "crypto_trades") \
        .option("startingOffsets", "latest") \
        .option("kafka.request.timeout.ms", "120000") \
        .option("kafka.session.timeout.ms", "60000") \
        .option("kafka.default.api.timeout.ms", "120000") \
        .option("failOnDataLoss", "false") \
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
            col("data.p").cast("double").alias("price"),
            col("data.q").cast("double").alias("quantity"),
            (col("data.p").cast("double") * col("data.q").cast("double")).alias("total_value"),
            when(col("data.m") == True, "SELL").otherwise("BUY").alias("trade_type"),
            (col("data.E") / 1000).cast("long").cast("timestamp").alias("timestamp")
        ).filter(col("total_value") > 10000)
        
    # Group by time window (1 minute, sliding every 10 seconds)
    sliding_df = parsed_df \
        .groupBy(
            window(col("timestamp"), "1 minute", "10 seconds"),
            col("symbol"),
            col("trade_type")
        ) \
        .agg(_sum("total_value").alias("total_volume"))
        
    # Write results to PostgreSQL
    db_url = f"jdbc:postgresql://postgres:5432/{os.environ.get('POSTGRES_DB')}"
    db_properties = {
        "user": os.environ.get("POSTGRES_USER"),
        "password": os.environ.get("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }
    
    def write_to_postgres_sliding(batch_df, batch_id):
        # Split the window column into 2 start/end columns
        flat_batch_df = batch_df.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("trade_type"),
            col("total_volume")
        )

        flat_batch_df.write \
            .option("truncate", "true") \
            .jdbc(
                url=db_url,
                table="sliding_wd_trade",
                mode="append",
                properties=db_properties
            )

    def write_to_postgres_raw(batch_df, batch_id):
        batch_df.write \
            .option("truncate", "true") \
            .jdbc(
                url=db_url,
                table="raw_trade",
                mode="append",
                properties=db_properties
            )
    
    sliding_query = sliding_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_postgres_sliding) \
        .option("checkpointLocation", "/tmp/checkpoints/sliding") \
        .start()

    raw_query = parsed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_postgres_raw) \
        .option("checkpointLocation", "/tmp/checkpoints/raw") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()