from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

from config import DATA_DIR

def formattedCustomerSpend(customer, amount):
    stringKey = str(customer)
    prettyValue = '{0:.2f}'.format(amount)
    return f'{customer}\t{prettyValue}'

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

# note including the date field which is not used; after experiments it appears 
# you are allowed to omit field from the end of the file (1800.csv has 7 cols)
# but you are not allowed to skip fields in the middle of the struct's field list
schema = StructType([ \
                     StructField("customerId", IntegerType(), True), \
                     StructField("orderId", IntegerType(), True), \
                     StructField("amount", FloatType(), True)])

# read the file as dataframe
df = spark.read.schema(schema).csv(f"{DATA_DIR}/customer-orders.csv")

# select relevant data only
customerSpendList = df.select("customerId", "amount")

# aggregate purchases by customer then sort
aggregatedCustomerSpend = customerSpendList.groupBy("customerId").sum("amount")
aggregatedSorted = aggregatedCustomerSpend.sort("sum(amount)", ascending=False)

# execute 
amountsSpent = aggregatedSorted.collect()
topCustomerList = amountsSpent[0:20]

print("ID \tAmount")
for customer in topCustomerList:
  print(formattedCustomerSpend(*customer))
  
# instructor solution ======
# his does better job of consolidatin the functions & utilizes native pretty printing
# Create schema when reading customer-orders
# customerOrderSchema = StructType([ \
#                                   StructField("cust_id", IntegerType(), True),
#                                   StructField("item_id", IntegerType(), True),
#                                   StructField("amount_spent", FloatType(), True)
#                                   ])

# # Load up the data into spark dataset
# customersDF = spark.read.schema(customerOrderSchema).csv(f'{DATA_DIR}/customer-orders.csv')

# totalByCustomer = customersDF.groupBy("cust_id").agg(func.round(func.sum("amount_spent"), 2) \
#                                       .alias("total_spent"))

# totalByCustomerSorted = totalByCustomer.sort("total_spent", ascending=False)

# totalByCustomerSorted.show(totalByCustomerSorted.count())
    
spark.stop()
                                              