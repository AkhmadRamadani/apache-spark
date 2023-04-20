from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Accessing Metadata").getOrCreate()

spark.catalog.listDatabases().select("name").show()

spark.catalog.listTables.show()

spark.catalog.isCached("sample_07")

spark.catalog.listFunctions().show() 
