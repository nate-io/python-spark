from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

from config import DATA_DIR

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

raw = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv(f'{DATA_DIR}/fakefriends-header.csv')

friendsByAge = raw.select("age", "friends")

# # group then calculate average over the group - my original solution
# friendsByAge.groupBy("age").avg("friends").show()

# # sort
# friendsByAge.groupBy("age").avg("friends").sort("age").show()

# # pretty printed 
# friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# add custom column name 
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)
    .alias("friends_avg")).sort("age").show()

spark.stop()