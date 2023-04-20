from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Data Source").getOrCreate()

df_txt = spark.read.text("../resources/people.txt")

df_txt.show()

df_txt
