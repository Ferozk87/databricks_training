# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Reading csv file

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True)

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/FileStore/tables/formula1/circuits.csv`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Transformation

# COMMAND ----------

Renaming column and add new column as ingestion, drop url

# COMMAND ----------

from pyspark.sql.functions import *
df1=(df.withColumnRenamed("circuitId","circuit_id")
.withColumnRenamed("circuitRef","circuit_ref")
.withColumn("ingestion_Date",current_timestamp())
.drop("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: writing to table

# COMMAND ----------

df1.write.saveAsTable("naval.circuit")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3B: writing to parquet FILE

# COMMAND ----------

df1.write.parquet("dbfs:/FileStore/tables/processedformual1/naval/cirucit")

# COMMAND ----------

# MAGIC %md
# MAGIC #### verfify parquet

# COMMAND ----------

df=spark.read.parquet("dbfs:/FileStore/tables/processedformual1/naval/cirucit")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from file_format.`path`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/FileStore/tables/processedformual1/naval/cirucit`

# COMMAND ----------


