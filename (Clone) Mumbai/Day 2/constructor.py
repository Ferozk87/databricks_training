# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/formula1/

# COMMAND ----------

df=spark.read.json("dbfs:/FileStore/tables/formula1/constructors.json")

# COMMAND ----------

Create Table tblenaem As Select

# COMMAND ----------

# MAGIC %sql
# MAGIC create table naval.constructor as 
# MAGIC (select constructorId as constructor_id, constructorRef as constructor_ref, name, nationality, current_timestamp() as ingestion_date from json.`dbfs:/FileStore/tables/formula1/constructors.json`)

# COMMAND ----------


