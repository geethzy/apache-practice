// Start Spark session
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
  .appName("Spark SQL Example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

import spark.implicits._

// Create RDD from a collection
val data = Array(1, 2, 3, 4, 5)
val rdd = spark.sparkContext.parallelize(data)

// Create DataFrame from RDD
val df = rdd.toDF("value")

// Perform DataFrame operations
df.select("value").show()
df.filter($"value" > 2).show()
df.groupBy("value").count().show()

// Create another DataFrame and join
val otherDf = spark.createDataFrame(Seq((1, "one"), (2, "two"))).toDF("number", "word")
df.join(otherDf, df("value") === otherDf("number")).show()
