from pyspark import SparkConf, SparkContext
from config import DATA_DIR

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile(f"{DATA_DIR}/book.txt")
# using flatMap here because of the book file structure
# it is essentially a list of lists where each sublist is 
# a sentence or a paragraph; in order to countByValue we need
# a flat lis of values
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    # ensure we don't encounter utf issues
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
