from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

from config import DATA_DIR

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

# note including the date field which is not used; after experiments it appears 
# you are allowed to omit field from the end of the file (1800.csv has 7 cols)
# but you are not allowed to skip fields in the middle of the struct's field list
schema = StructType([ \
                     StructField("stationID", StringType(), True), \
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True), \
                     StructField("temperature", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv(f"{DATA_DIR}/1800.csv")
df.printSchema()

# FIELD LIST GAP TESTING 
# toggle date from the schema, run this, and see the results are None
# restore date and data prints as expected
# tempRows = df.select("stationID", "measure_type", "temperature").limit(20)
# rowsToPrint = tempRows.collect()

# for row in rowsToPrint:
#     print(row[0], row[1])

# Filter out all but TMIN entries
minTemps = df.filter(df.measure_type == "TMIN")

# Select only stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# Aggregate to find minimum temperature for every station
minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
minTempsByStation.show()

# Convert temperature to fahrenheit and sort the dataset
minTempsByStationF = minTempsByStation.withColumn("temperature",
                                                  func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                                                  .select("stationID", "temperature").sort("temperature")
                                                  
# Collect, format, and print the results
results = minTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
    
spark.stop()

                                                  