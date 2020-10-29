from pyspark.sql import SparkSession
from pyspark.sql import functions as func

from config import DATA_DIR

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of my book into a dataframe
inputDF = spark.read.text(f"{DATA_DIR}/book.txt")

# Split using a regular expression that extracts words
# func.split seperates the lines, func.explode converts to rows
# note since we do not name the column or provide a schema the column is named 'value'
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))

# data cleansing
words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = words.select(func.lower(words.word).alias("word"))

# Count up the occurrences of each word - aggregation step
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts which depends on previous step
wordCountsSorted = wordCounts.sort("count")

# Show the results
# w/o the count portion you'd just get a flat list of the unique words
wordCountsSorted.show(wordCountsSorted.count())
