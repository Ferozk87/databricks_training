# Databricks notebook source
# MAGIC %run "/Workspace/Users/feroz.khan01@marsh.com/Day 2/includes"

# COMMAND ----------

df=spark.read.json(f"{location}/constructors.json")
df_final=df.drop("url").withColumn("ingestion_time",current_timestamp())

# COMMAND ----------

df_final.write.mode("overwrite").parquet(f"{output_path}feroz/constructor")

# COMMAND ----------

df=spark.read.parquet(f"{output_path}feroz/constructor")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from parquet.`dbfs:/mnt/cloudthats3/output_formula1/feroz/constructor`
