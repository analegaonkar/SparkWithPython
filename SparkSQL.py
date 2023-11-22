#Load a data file into a dataframe
#Create a Table View for the dataframe
#Run basic SQL queries and aggregate data on the table view
#Create a Pandas UDF to perform columnar operations

import findspark
findspark.init()

import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

#Create SparkContext and SparkSession
sc = SparkContext()

spark = SparkSession \
        .builder \
        .appName("Python Spark DataFrame tutorial") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

#Intialize spark session
#spark

#Loading data into Pandas dataframe

mtcars = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/mtcars.csv')

#mtcars.head()

mtcars.rename(columns={'Unnamed: 0':'name'}, inplace=True)

#loading data into Spark dataframe

sdf = spark.createDataFrame(mtcars)

sdf.printSchema()

sdf_new = sdf.withColumnRenamed('vs','versus')
sdf_new.printSchema()

#Create a table view

sdf_new.createOrReplaceTempView('cars')

#Running SQL queries and aggregations on the table view

#spark.sql("SELECT mpg FROM cars").show()

#spark.sql("SELECT * FROM cars where mpg>20 and cyl<6").show()

#spark.sql("SELECT count(*),cyl FROM cars GROUP BY cyl").show()

#sdf.where("mpg>20").where("cyl<6").show()

#sdf.where(sdf['mpg']<18).show()

#Create Pandas UDF to apply columnar operations

from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyarrow

#define a function to convert weight from imperial to metric
@pandas_udf("float")

def convert_wt(s: pd.Series) -> pd.Series:
    #formula to convert imperial to metric tons
    return s*0.45

spark.udf.register("convert_weight", convert_wt)

#Apply the UDF to the "wt" column table view

spark.sql("SELECT *, wt as weight_imperial, convert_weight(wt) as weight_metric FROM cars").show()
