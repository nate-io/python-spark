# [Big Data With Spark &amp; Python](https://www.udemy.com/course/taming-big-data-with-apache-spark-hands-on) 

The devlopment environment is Spark setup on top of a JDK in a Python environment with [miniconda](https://docs.conda.io/en/latest/miniconda.html). Hadoop is mocked with the [winutils package](https://github.com/steveloughran/winutils).

Datasets used:
* Local machine processing ml-100k found [here](https://grouplens.org/datasets/movielens/100k/).

[Windows setup instructions](https://sundog-education.com/spark-python/); follow explicitly because Windows requires workarounds to run Spark of course. 😢


## Notes

### Resilient Distribuited Datasets (RDDs)

Use lazy evaluation - nothing happens in drvier program until an action is taken. Once an action is taken Spark can build the directed acyclic graph (which allows it to be generally more efficient than MapReduce).

* Common RDD transforms
  - map
  - flatmap
  - filter
  - distinct
  - sample
  - union, intersection, subtract, cartesian
* Common RDD actions
  - collect
  - count
  - countByValue
  - take
  - top
  - reduce
* RDD key/value SQL-style joins
  - join, rightOuterJoin, leftOuterJoin, cogroup, subtractByKey
* Common RDD key/value actions
  - reduceByKey
  - groupByKey
  - sortByKey
  - keys(), values()
  - note: if not transforming the keys of the k/v RDD, use mapValues()/flatMapValues() instaed of map/flatMap because it is more efficient