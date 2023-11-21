#Create the SparkContext and SparkSession
#Create an RDD and apply some basic transformations and actions to RDDs
#Demonstrate the use of the basics Dataframes and SparkSQL

# findspark will locate spark and import it as a regular library
import findspark
import time
findspark.init()

# PySpark is the Spark API for Python. In this lab, we use PySpark to initialize the spark context.
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

#Creating a spark context class - entry point for spark app and conatins functions to create RDDs
sc = SparkContext()

#Create a spark session - needed for sparksql and dataframe operations
spark = SparkSession \
        .builder \
        .appName("Python Spark Data Frames basic practical") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

#Initialize spark session

spark

#Create a RDD
data = range(1,30)
print(data[0])
len(data)
dataRDD = sc.parallelize(data,4)

print(dataRDD)

#Transformations - square the numbers and filter RDD to elements t< 10

subRDD = dataRDD.map(lambda x : x * x)
filteredRDD = subRDD.filter(lambda x : x<10)

#tranformation return result to driver. Get output using collect()
print(filteredRDD.collect())
filteredRDD.count()

#Caching Data 
test = sc.parallelize(range(1,50000),4)
test.cache()

t1=time.time()
count1 = test.count()
dt1 =time.time() - t1
print("dt1: ",dt1)


t2=time.time()
count2 = test.count()
dt2 =time.time() - t2
print("dt2: ", dt2)












