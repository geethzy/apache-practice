//spark-shell

// Create RDD from a collection
val data = Array(1, 2, 3, 4, 5)
val rdd = sc.parallelize(data)
//val rddFromFile = sc.textFile("path/to/your/file.txt")

// Perform transformations
val rddMapped = rdd.map(x => x * 2)
val rddFiltered = rdd.filter(x => x % 2 == 0)
val rddFlatMapped = rdd.flatMap(x => Array(x, x * 2, x * 3))

// Perform actions
val sum = rdd.reduce((a, b) => a + b)
val allElements = rdd.collect()
val count = rdd.count()

// Print results
println(s"Sum: $sum")
println(s"All Elements: ${allElements.mkString(", ")}")
println(s"Count: $count")


