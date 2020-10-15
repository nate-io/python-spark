from pyspark import SparkConf, SparkContext

from config import DATA_DIR

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

# parse the data file
lines = sc.textFile(f"{DATA_DIR}/1800.csv")
parsedLines = lines.map(parseLine)

# filter data for rows which are TMAX observations
minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
# construct tuples where k is station and v is temp
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# find the max temp for each unique key
minTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))

# take action (Spark finally does work here, DAG constructed from above)
results = minTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
