from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Spark SQL Example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Create an RDD
data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)

# Create DataFrame from RDD with schema
df = rdd.map(lambda x: (x,)).toDF(["value"])

# Create another DataFrame for join operation
other_data = [(1, "one"), (2, "two"), (6, "six")]
other_df = spark.createDataFrame(other_data, ["number", "word"])

# Create temporary views
df.createOrReplaceTempView("values")
other_df.createOrReplaceTempView("numbers")

# Run SQL queries on the temporary views
result_df = spark.sql("SELECT * FROM values")
result_df.show()

filtered_df = spark.sql("SELECT * FROM values WHERE value > 2")
filtered_df.show()

grouped_df = spark.sql("SELECT value, COUNT(*) as count FROM values GROUP BY value")
grouped_df.show()

# Join operation using SQL
join_df = spark.sql("""
    SELECT v.value, n.word
    FROM values v
    LEFT JOIN numbers n
    ON v.value = n.number
""")
join_df.show()
