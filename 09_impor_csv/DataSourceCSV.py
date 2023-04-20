from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Data Source").getOrCreate()
csv_df = spark.read.options(header='true',inferSchema='true').csv("../resources/cars.csv")

csv_df.printSchema()

csv_df.show()

# create new DataFrame with selected columns
csv_df2 = csv_df.select("Year", "Make", "Model", "Horsepower")

csv_df2.show()
