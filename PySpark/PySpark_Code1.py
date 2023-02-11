# locate the Spark installation
import findspark
findspark.init() 
#Importing Spark Session
from pyspark.sql import SparkSession
#Importing PySpark types for DataFrame use
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Creating the SparkSession in Local Machine with 2 Treads
spark = SparkSession.builder.master("local[2]").appName('SampleApp').getOrCreate()

#The following command provides the SparkUI Details in the PySpark application
print(spark.sparkContext.uiWebUrl)

#Creating list of Rows for creating the DataFrame
myList = [Row('Siva',1000,10000.00),
          Row('Krishna',2000,200000.00),
          Row('yash',500,10000.00)]

# Creating the Explicit Schema for the List of Rows
mySchema = """Name string, id int, salary float"""

#Create DataFrame using the above list and schema and Transform the data
df_list = spark.createDataFrame(myList,mySchema)
df1 = df_list.select("*",lit(current_timestamp()).alias('TimeStamp'))
df1.withColumn('rownumber',lit(monotonically_increasing_id()))
# Get the  Number of Partitions before Repartition
print(df1.rdd.getNumPartitions())

df2 = df1.coalesce(1).sort(col('salary').desc())

# Get the  Number of Partitions After Repartition
print(df2.rdd.getNumPartitions())

df2.show()

#Provide the explain plan of written code
df2.explain(extended=True)

#Stop the Spark Session
spark.stop()

