from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Kafka HDFS Structured Streaming Example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "geetopic"

# Read data from Kafka
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", kafka_topic) \
  .load()

# Define HDFS sink options
hdfs_path = "hdfs://localhost:9000/path/to/save/data/"

# Write data to HDFS in append mode
query = df \
    .writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", hdfs_path) \
    .option("checkpointLocation", "/tmp/checkpoint_location") \
    .start()

# Await termination (or you can set a timeout)
query.awaitTermination()


# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 structured_streaming_example.py
