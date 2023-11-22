from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

sc = SparkContext()
spark = SparkSession.builder.appName("DataFrames").getOrCreate()


#Understanding Join operations on dataframes
data = [("A101", "John"), ("A102", "Peter"), ("A103", "Charlie")] 

columns = ["emp_id", "emp_name"]

sp_df1 = spark.createDataFrame(data, columns)


data = [("A101", 3250), ("A102", 6735), ("A103", 8650)] 

columns = ["emp_id", "salary"] 

sp_df2 = spark.createDataFrame(data, columns) 

# create a new dataframe joining the two dataframes on the emp_id column

sp_combined_df = sp_df1.join(sp_df2, on = "emp_id", how="inner")

#sp_combined_df.collect()


# define sample DataFrame 1 with some missing values

data = [("A101", 1000), ("A102", 2000), ("A103",None)]

columns = ["emp_id", "salary"]

dataframe_1 = spark.createDataFrame(data, columns)

# fill missing salary value with a specified value

filled_df = dataframe_1.fillna({"salary": 3000})

