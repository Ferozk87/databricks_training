# Databricks notebook source
# MAGIC %md
# MAGIC schema 
# MAGIC when
# MAGIC 1. if you have large data 
# MAGIC 2. do not have header
# MAGIC
# MAGIC
# MAGIC which
# MAGIC 1.str/list
# MAGIC 2. pyspark datatype

# COMMAND ----------

# MAGIC %md
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html

# COMMAND ----------

# MAGIC %md
# MAGIC Special Spark Datatype
# MAGIC 1. StructType
# MAGIC 2. Array Type
# MAGIC 3. Map Type
# MAGIC

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run "/Workspace/Users/feroz.khan01@marsh.com/Day 2/includes"

# COMMAND ----------

# MAGIC %md
# MAGIC {"driverId":1,"driverRef":"hamilton","number":44,"code":"HAM","name":{"forename":"Lewis","surname":"Hamilton"},"dob":"1985-01-07","nationality":"British","url":"http://en.wikipedia.org/wiki/Lewis_Hamilton"}

# COMMAND ----------

user_schema=StructType([StructField("driverId",IntegerType()),
                        StructField("driverRef",StringType()),
                        StructField("number",IntegerType()),
                        StructField("code",StringType()),
                        StructField("name",MapType(StringType(),StringType())),
                        StructField("dob",StringType()),
                        StructField("nationality",StringType()),
                        StructField("url",StringType())
                        ])

# COMMAND ----------

df_auto=spark.read.json(f"{location}/drivers.json")

# COMMAND ----------

display(df_auto)

# COMMAND ----------

df_auto.withColumn("forename",col("name.forename"))\
.withColumn("surname",col("name.surname"))\
    .drop("name")\
.display()

# COMMAND ----------

df=spark.read.schema(user_schema).json(f"{location}/drivers.json")

# COMMAND ----------

df.withColumn("forename",col("name.forename"))\
.withColumn("surname",col("name.surname"))\
    .drop("name")\
.display()

# COMMAND ----------

display(df)

# COMMAND ----------

ddf=spark.read.json("dbfs:/mnt/cloudthats3/raw_json/array.json").display()

# COMMAND ----------

schema_arr=StructType([StructField("id", IntegerType()),
                                  StructField("mobile" , ArrayType(IntegerType())),
                                   StructField("name",  StringType())
                                               ])




# COMMAND ----------

schema_array=StructType([StructField("id",IntegerType()),
                         StructField("mobile",ArrayType(IntegerType())),
                         StructField("name",StringType())

])
