from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName("Creating DataFrames").getOrCreate()

# Create DataFrame
mylist = [(1, "Nama-NIM"),(3, "Big Data 2023")]
myschema = ['col1', 'col2']
df = spark.createDataFrame(mylist, myschema)

#Convert DF to RDD
df.rdd.collect()

df2rdd = df.rdd
df2rdd.take(2)

# save RDD to file
df2rdd.saveAsTextFile("output/df2rdd.txt")

# Convert RDD to DF
rdd2df = df2rdd.toDF(myschema)
rdd2df.show()
