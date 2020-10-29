from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

from config import DATA_DIR

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

raw = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv(f'{DATA_DIR}/fakefriends-header.csv')

# throwaway unneeded data for more efficient processing (and saving cluster resources)
friendsByAge = raw.select("age", "friends")

# v1 group then calculate average over the group - my original solution
# friendsByAge.groupBy("age").avg("friends").show()

# v2 add sort
# friendsByAge.groupBy("age").avg("friends").sort("age").show()

# v3 pretty print numerical values
# friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# v4 add custom column name 
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)
    .alias("friends_avg")).sort("age").show()

# v5 syntax experiment
# (friendsByAge
#     .groupBy("age")
#     .agg(func.round(func.avg("friends"), 2).alias("friends_avg"))
#     .sort("age")
#     .show())

spark.stop()