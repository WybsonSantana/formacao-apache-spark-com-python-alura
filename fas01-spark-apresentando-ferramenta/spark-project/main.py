import findspark
from pyspark.sql import SparkSession

findspark.init()

spark = SparkSession.builder.master('local[*]').getOrCreate()
print(spark)
