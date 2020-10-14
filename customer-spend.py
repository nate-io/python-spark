from pyspark import SparkConf, SparkContext
from config import DATA_DIR

conf = SparkConf().setMaster("local").setAppName("CustomerSpend")
sc = SparkContext(conf = conf)

def parseOrder(order):
    fields = order.split(',')
    customerId = int(fields[0])
    amount = float(fields[2])
    return (customerId, amount)

def formattedCustomer(customer):
    k, v = customer
    prettyValue = '{0:.2f}'.format(v)
    return f'{k} {prettyValue}'

raw = sc.textFile(f"{DATA_DIR}/customer-orders.csv")
customerPurchases = raw.map(parseOrder).reduceByKey(lambda x, y: x + y)

# sort & output

# v1 sort by key
# sortedResults = sorted(customerPurchases.collect())

# v2 sortBy value to see who spent the most
# lambda consumes tuples so sort by the second element
sortedCustomers = customerPurchases.sortBy(lambda x: x[1])
sortedResults = sortedCustomers.collect()

for result in sortedResults:
    print(formattedCustomer(result))