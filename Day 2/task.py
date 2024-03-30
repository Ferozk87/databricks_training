# Databricks notebook source
# MAGIC %md
# MAGIC dbfs:/mnt/cloudthats3/raw_json/restaurant_details.json

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/cloudthats3/raw_json/restaurant_details.json",multiLine=True)

# COMMAND ----------

df.display()
