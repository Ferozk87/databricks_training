# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/

# COMMAND ----------

df=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/",header=True)

# COMMAND ----------

df=spark.read.csv("dbfs:/databricks-datasets/asa/airlines/",header=True,inferSchema=True)

# COMMAND ----------

User defined schema
Ways
1. Str
[2. List]
2. Pyspark datatypes(struct)

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True)

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

user_Scehma="circuitId integer, circuitRef string, name string, location string, country string,lat double, lng double, alt integer, url string"

# COMMAND ----------

df=spark.read.schema(user_Scehma).csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True)

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

users_schem_pyspark=StructType([StructField("circuitId",IntegerType()),
                                StructField("circuitRef",StringType()),
                                StructField("name",StringType()),
                                StructField("location",StringType()),
                                StructField("country",StringType()),
                                StructField("lat",DoubleType()),
                                StructField("lng",DoubleType()),
                                StructField("alt",IntegerType()),

])

# COMMAND ----------

df_new=spark.read.schema(users_schem_pyspark).csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True)

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/formula1

# COMMAND ----------


