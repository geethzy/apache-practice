from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("Kafka to MySQL") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "your_topic") \
  .load()

schema = StructType().add("key", StringType()).add("value", StringType())

parsed_df = df \
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .withColumn("data", from_json(col("value"), schema)) \
  .select("data.*")

schema = StructType().add("key", StringType()).add("value", StringType())

parsed_df = df \
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .withColumn("data", from_json(col("value"), schema)) \
  .select("data.*")

# Example MySQL JDBC URL format
jdbc_url = "jdbc:mysql://localhost:3306/your_database"

# Example MySQL connection properties
connection_properties = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.mysql.jdbc.Driver"
}

# Write data to MySQL sink
query = parsed_df \
    .writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write.jdbc(jdbc_url, "table_name", mode="append", properties=connection_properties)) \
    .start()

query.awaitTermination()

# spark-submit kafka_to_mysql.py