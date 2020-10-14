from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

# parse file & generate raw data list
# data is simulated social network user list, with their age and # of friends
lines = sc.textFile("file:///C:/Users/nyoun/repos/python-spark/data/fakefriends.csv")
rdd = lines.map(parseLine)

# aggregate data such that the output is a k/v list where
# k == the age
# v == tuple (sum of friends for this age, number of users with this age)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# reduce the age value list to display a mean number of friends for each age 
averagesByAge = totalsByAge.mapValues(lambda x: x[0] // x[1])
results = sorted(averagesByAge.collect())

# output
for result in results:
    print(result)
