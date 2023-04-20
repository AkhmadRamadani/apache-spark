

from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName("Creating DataFrames").getOrCreate()

mylist = [(50, "DataFrame"),(60, "pandas")]

myschema = ['col1', 'col2']

# Method1: Create a DataFrame with a list, schema and default data types.

df1 = spark.createDataFrame(mylist, myschema)

# Method2: Create a DataFrame by parallelizing list and convert the RDD to DataFrame.
sc = spark.sparkContext

df2 = sc.parallelize(mylist).toDF(myschema)

# Method3: Read data from a database table, Infer schema and convert to DataFrame.
df_people = spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:8889/katteri_db",
    dbtable="users").load()

df_people.createOrReplaceTempView("users")

spark.sql("SHOW TABLES").show()

spark.sql("SELECT * FROM users").show()




