from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# create SparkSession
spark = SparkSession.builder.appName("dept-app").getOrCreate()
sc = spark.sparkContext

# create schema for department data
dept_schema = StructType([
    StructField("dept_id", IntegerType(), True),
    StructField("dept_name", StringType(), True)
])

# create RDD for department data
dept_rdd = sc.parallelize([(1, "Sales"), (2, "HR")])

# create DataFrame for department data using RDD and schema
dept_df = spark.createDataFrame(dept_rdd, schema=dept_schema)

# define a case class for the department data
class Dept:
    def __init__(self, dept_id, dept_name):
        self.dept_id = dept_id
        self.dept_name = dept_name
        
# convert DataFrame to Dataset
dept_ds = dept_df.rdd.map(lambda row: Dept(row.dept_id, row.dept_name)).toDS()

# show Dataset
dept_ds.show()