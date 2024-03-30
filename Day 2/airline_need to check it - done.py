# Databricks notebook source
# MAGIC %run "/Workspace/Users/feroz.khan01@marsh.com/Day 2/includes"

# COMMAND ----------

# MAGIC
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/")
df.limit(5).display()

# COMMAND ----------

schema_names=StructType([StructField("Year", IntegerType()),
                         StructField("Month", IntegerType()),
                         StructField("DayofMonth", IntegerType()),
                         StructField("DayOfWeek", IntegerType()),
                         StructField("DepTime", IntegerType()),
                         StructField("CRSDepTime", IntegerType()),
                         StructField("ArrTime", StringType()),
                         StructField("CRSArrTime", IntegerType()),
                         StructField("UniqueCarrier", StringType()),
                         StructField("FlightNum", IntegerType()),
                         StructField("TailNum", StringType()),
                         StructField("ActualElapsedTime",IntegerType()),
                         StructField("CRSElapsedTime",IntegerType()),
                         StructField("AirTime", StringType()),
                         StructField("ArrDelay", IntegerType()),
                         StructField("DepDelay", IntegerType()),
                         StructField("Origin",StringType()),
                         StructField("Dest",StringType()),                        
                         StructField("Distance",IntegerType()),
                         StructField("TaxiIn",StringType()),
                         StructField("TaxiOut",StringType()),                         
                         StructField("Cancelled",IntegerType()),
                         StructField("CancellationCode",StringType()),
                         StructField("Diverted",IntegerType()),                       
                         StructField("CarrierDelay",StringType()),
                         StructField("WeatherDelay",StringType()),                         
                         StructField("NASDelay",IntegerType()),
                         StructField("LateAircraftDelay",StringType())
                              ])
                         

# COMMAND ----------

df=spark.read.schema(schema_names).csv("dbfs:/databricks-datasets/asa/airlines/")

# COMMAND ----------

df.display()
