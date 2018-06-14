# -*- coding: utf-8 -*-
"""
Created on Mon Jun  4 14:25:38 2018

@author: Shabaka
"""


import pyspark as spark
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.files import SparkFiles
from pyspark import SparkConf, SparkContext
conf = (SparkConf().setMaster("local").setAppName("My app") .set("spark.executor.memory", "1g"))

sc = SparkContext(conf=conf)


# #### Introductory Notes to keep in mind on Spark ######
# Spark is a platform for cluster computing. Spark lets you spread data and
# computations over clusters with multiple nodes
# (think of each node as a separate computer).
# Splitting up your data makes it easier to work with very large datasets
# because each node only works with a small amount of data.

# As each node works on its own subset of the total data, it also carries out
# a part of the total calculations required, so that both data processing and
# computation are performed in parallel over the nodes in the cluster.
# It is a fact that parallel computation can make certain types of
# programming tasks much faster.

# consider questions like:

# Is my data too big to work with on a single machine?
# Can my calculations be easily parallelized?

# ### Step 1 in Using Spark - Connect to a cluster (hosted on a remote machine)

#%%

# ###### Create connection by creating instance of SparkContext class
# to take in optional arguments that allow specificity of attributes of cluster

# ### We hold the attributes using the SparkConf() constructor

# For the implementations to follow - Our SparkContext class is called sc

# ###  Interesting note for reference
# https://sigdelta.com/blog/how-to-install-pyspark-locally/

#%%

# ##### Verify the Spark Contexct Class ####### #

sc = spark.sparkContext
# Verify SparkContext
print(sc)

# Print Spark version
print(sc.version)

#%%
# #### Working with the Spark Dataframe - More flexible than RDDs ####

# Spark dataframes have optimisations built in - a bit like the SQL tables ##

# To start working with Spark DataFrames, you first have to create a
# SparkSession object from your SparkContext

# The sparkcontext is essentially the connection to the cluster
# The sparksession is the interface with the connection

# The spark session in this implementation is called "spark"
# However, if not sure about session availability - to mitigate multiple
# instances of Sparksessions and Sparkcontext

# Best practice to run the SparkSession.builder.getorCreate()
# ##### ##############  #################   ######### ####### ####

# ### Create a SparkSession #######

# import pyspark
# from pyspark.sql import SparkSession

# Create my_spark
my_spark = SparkSession.builder.getOrCreate()

# Print my_spark - this verifies it is a spark session
print(my_spark)

#%%
# ### See what data exists in the cluster #####

# ### View Tables ####

# We use the Sparksession catalog attribute to extract different
# pieces of information

# Print the tables in the catalog
print(spark.catalog.listTables())

#%%
# ### Maka a query ###

query = "FROM flights SELECT * LIMIT 10"

# Get the first 10 rows of flights
flights10 = spark.sql(query)

# Show the results
flights10.show()

# ##### PySpark Dataframe to Pandas #####

query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"

# Run the query
flight_counts = spark.sql(query)

# Convert the results to a pandas DataFrame
pd_counts = flight_counts.toPandas()

# Print the head of pd_counts
print(pd_counts.head())

#%%

# #### Pandas to Spark Dataframe ####

# Create pd_temp
pd_temp = pd.DataFrame(np.random.random(10))

# Create Spark dataframe spark_temp from pd_temp
spark_temp = spark.createDataFrame(pd_temp)

# Examine the tables in the catalog. These will not include the new temp frame
print(spark.catalog.listTables())

# Add spark_temp to the catalog
spark_temp.createOrReplaceTempView("temp")

# Examine the tables in the catalog again
print(spark.catalog.listTables())

#%%

# ##### Spark Dataframe from csv file ####

# This could be any csv file path
file_path = "/usr/local/share/datasets/airports.csv"

# Read in the airports data
airports = spark.read.csv(file_path, header=True)

# Show the data
airports.show()

#%%

# ### You might want to create a new column #####

# New column must be an object class Column.

# Example

# Create the DataFrame flights
flights = spark.table("flights")

# Show the head
print(flights.show())

# Add duration_hrs
flights = flights.withColumn("duration_hrs", flights.air_time/60)

#%%

# ######### Filtering Data for insights  #########
# Method 1 - Filter flights with a SQL string
long_flights1 = flights.filter("distance > 1000")

# Method 2 - Filter flights with a boolean column
long_flights2 = flights.filter(flights.distance > 1000)

# Proof of Implementation - Output equality check
print(long_flights1.show())
print(long_flights2.show())

#  ### Column Selections - 1 #######

# Select the first set of columns
selected1 = flights.select("tailnum", "origin", "dest")

# Select the second set of columns
temp = flights.select(flights.origin, flights.dest, flights.carrier)

# Define first filter
filterA = flights.origin == "SEA"

# Define second filter
filterB = flights.dest == "PDX"

# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)

#%%

#  ### Column Selections - 2 #######

# Define avg_speed
avg_speed = (flights.distance/(flights.air_time/60)).alias("avg_speed")

# Select the correct columns
speed1 = flights.select("origin", "dest", "tailnum", avg_speed)

# Create the same table using a SQL expression
speed2 = flights.selectExpr("origin", "dest", "tailnum",
                            "distance/(air_time/60) as avg_speed")

#%%

# ######    Aggregation Example ### #########

# Find the shortest flight from PDX in terms of distance
flights.filter(flights.origin == "PDX").groupBy().min("distance").show()

# Find the longest flight from SEA in terms of duration
flights.filter(flights.origin == "SEA").groupBy().max("air_time").show()

#%%
# average air time of Delta Airlines flights (where the carrier column has the
# value "DL") that left SEA

flights.filter(flights.carrier == "DL").filter(flights.origin == "SEA").groupBy().avg().show()

#%%

# Average duration of Delta flights
flights.filter(flights.carrier == "DL").filter(flights.origin == "SEA").groupBy().avg("air_time").show()

# Total hours in the air
flights.withColumn("duration_hrs", flights.air_time/60).groupBy().sum("duration_hrs").show()

#%%

# ######## Grouping and Aggregating #########
# Group by tailnum
by_plane = flights.groupBy("tailnum")

# Number of flights each plane made
by_plane.count().show()

# Group by origin
by_origin = flights.groupBy("origin")

# Average duration of flights from PDX and SEA
by_origin.avg("air_time").show()

# #### More Aggregating Examples ###### #

# Import pyspark.sql.functions as F - This is at top of script (Best practice)

# Group by month and dest
by_month_dest = flights.groupBy("month", "dest")

# Average departure delay by month and destination
by_month_dest.avg("dep_delay").show()

# Standard deviation
by_month_dest.agg(F.stddev("dep_delay")).show()

#%%

# ###### Joins ######
# Examine the data
print(airports.show())

# Rename the faa column
airports = airports.withColumnRenamed("faa", "dest")

# Join the DataFrames
flights_with_airports = flights.join(airports, on="dest", how="leftouter")

# Examine the data again
print(flights_with_airports.show())


#%%
# ####### PySpark ML Pipeline #######

# For instance, we can rename the year column of planes to plane_year to
# avoid duplicate column names.

# to do this, we create a new DataFrame called model_data by
# joining the flights table with planes using the tailnum column as the key.

# Rename year column
planes = planes.withColumnRenamed("year", "plane_year")

# Join the DataFrames
model_data = flights.join(planes, on="tailnum", how="leftouter")

# ### By default, spark tends to work with numeric data #######
# SPark tries to guess what type of information we are working with
# and sometimes, this doesn't go to plan ####
# so we cast() methods or we can just use the .withColumn method
# However, it is worth noting that cast() is for columns and the latter for df

# usng a PCA format, we can extracrt from the data source appropriate cols
# of the data for our model build #####

# Cast the columns to integers
model_data = model_data.withColumn("arr_delay", model_data.arr_delay.cast("integer"))
model_data = model_data.withColumn("air_time",
                                   model_data.air_time.cast("integer"))
model_data = model_data.withColumn("month",
                                   model_data.month.cast("integer"))
model_data = model_data.withColumn("plane_year", model_data.plane_year.cast("integer"))

# Now we try to create a new column - Essentially creating a new variable
# to include in the model build

# Create the column plane_age
model_data = model_data.withColumn("plane_age", model_data.year - model_data.plane_year)

# So for instance , we try to answer a simple yes/no question ####
# for example was flight late or not? This will help us to decipher
# the chances of a flight being late in te future
# This defines the outcome column for the model - CBR ###

# Create is_late
model_data = model_data.withColumn("is_late", model_data.arr_delay > 0)

# Convert to an integer
model_data = model_data.withColumn("label", model_data.is_late.cast("integer"))

# Remove missing values
model_data = model_data.filter("arr_delay is not NULL and dep_delay is not NULL and air_time is not NULL and plane_year is not NULL")

# ###### Strings and factors #####