from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import json
import requests
#Create First DataFrame!

# Download the data first into a local `people.json` file
#!curl https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/people.json >> people.json

#response = json.loads(requests.get("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/people.json").text)

url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/people.json";
r = requests.get(url)

with open('people.json', 'w') as f:
    f.write(r.text)

#Creating a SparkSession
sc = SparkContext()

#Create a spark session
spark = SparkSession\
    .builder\
    .appName("SparkSQL and DataFrame") \
    .getOrCreate()


# Read the dataset into a spark dataframe using the `read.json()` function  
df = spark.read.json("people.json").cache()

# Print the dataframe as well as the data schema    
df.show()
df.printSchema()

#Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

#Explore the data using DataFrame functions and SparkSQL

# Select and show basic data columns
df.select("name").show()
df.select(df["name"]).show()

spark.sql("SELECT name FROM people").show()

#Perform basic filtering
df.filter(df["age"] > 21).show()

spark.sql("SELECT name FROM people WHERE age > 21").show()

#Perform basic aggregation of data
df.groupBy("age").count().show()
#+----+-----+
#| age|count|
#+----+-----+
#|  19|    1|
#|NULL|    1|
#|  30|    1|
#+----+-----+

spark.sql("SELECT age, COUNT(age) as count FROM people GROUP BY age").show()
#+----+-----+
#| age|count|
#+----+-----+
#|  19|    1|
#|NULL|    0|
#|  30|    1|
#+----+-----+