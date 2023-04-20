from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("JSON Reader").getOrCreate()

df = spark.read.json("../resources/people.json")

df.show()

# filter corrupt records
df.filter(df.age > 21).show()

