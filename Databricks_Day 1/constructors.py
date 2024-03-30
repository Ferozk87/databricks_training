# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1_raw/
# MAGIC ###dbfs:/FileStore/tables/formula1_raw/constructors.json
# MAGIC

# COMMAND ----------

df= spark.read.json("dbfs:/FileStore/tables/formula1_raw/constructors.json")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *
df1 = df.drop("url")
df2=df1.withColumn("Ingesion_Date",current_timestamp())

df2.write.saveAsTable("feroz.constructors")

# COMMAND ----------

spark.read.json("dbfs:/FileStore/tables/formula1_raw/constructors.json").withColumn("Ingesion_Date",current_timestamp()).drop("url").write.saveAsTable("feroz.constructors4")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json. `dbfs:/FileStore/tables/formula1_raw/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC create table feroz.contructor3 as 
# MAGIC select *, current_timestamp() as ingestion_date from json. `dbfs:/FileStore/tables/formula1_raw/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv. `dbfs:/FileStore/tables/formula1_raw/circuits.csv' OPTIONS (header 'True', inferSchema'True')
# MAGIC
# MAGIC ## this will not work
