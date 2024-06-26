from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark SQL Example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Create an RDD
data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)

# Perform transformations
rdd_mapped = rdd.map(lambda x: x * 2)
rdd_filtered = rdd.filter(lambda x: x % 2 == 0)
rdd_flatmapped = rdd.flatMap(lambda x: [x, x * 2, x * 3])

# Perform actions
sum_result = rdd.reduce(lambda a, b: a + b)
all_elements = rdd.collect()
count = rdd.count()

# Print results
print(f"Sum: {sum_result}")
print(f"All Elements: {all_elements}")
print(f"Count: {count}")

# Create DataFrame from RDD with schema
df = rdd.map(lambda x: (x,)).toDF(["value"])

# Perform DataFrame operations
df.select("value").show()
df.filter(df["value"] > 2).show()
df.groupBy("value").count().show()

# Create another DataFrame and join
other_df = spark.createDataFrame([(1, "one"), (2, "two")], ["number", "word"])
df.join(other_df, df["value"] == other_df["number"]).show()
