# Databricks notebook source
# MAGIC %fs  ls dbfs:/FileStore/tables/formula1_raw/

# COMMAND ----------

df= spark.read.csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2: Transformation
# MAGIC - Data Frame tranformation 

# COMMAND ----------

help(df.select)

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df.select("circuitId","name").display()

# COMMAND ----------

from pyspark.sql.functions import *   
df.select(col("name").alias("Full_name")).display()

# COMMAND ----------

df.select("circuitId", col("circuitRef"),df.name, df["location"]).display()

# COMMAND ----------

df.select(concat("location",lit(" "),"country").alias("country")).display()

# COMMAND ----------

    df.withColumnRenamed("name","Name").withColumnRenamed("country","Country").display()

# COMMAND ----------

df.columns

# COMMAND ----------

new_Column_names=['circuit_ID',
 'circuitRef',
 'Name',
 'Location',
 'Country',
 'lat',
 'lng',
 'alt',
 'URL']

# COMMAND ----------

df1=df.toDF(*new_Column_names)

# COMMAND ----------

    df1.display()

# COMMAND ----------

df1.drop("URL").display()
df2 = df1.drop("URL") # new data will be stored in df2 dataframe
df2.display()

# COMMAND ----------

df1.withColumn("Current_time", current_date()).display()

# COMMAND ----------

df1.withColumn("name",upper("name")).display()

# COMMAND ----------

df1.withColumn("New_name",upper("name")).display()

# COMMAND ----------

df.write.parquet("dbfs:/FileStore/tables/output/feroz/circuit")

# COMMAND ----------

df=spark.read.parquet("dbfs:/FileStore/tables/output/feroz/circuit").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists feroz;
# MAGIC

# COMMAND ----------

df1.write.parquet("dbfs:/FileStore/tables/output/feroz/circuit",mode="overwrite")

# COMMAND ----------

df1=spark.read.parquet("dbfs:/FileStore/tables/output/feroz/circuit")

# COMMAND ----------

df1.write.saveAsTable("feroz.circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC select location, count(circuit_ID) from feroz.circuit group by Location
