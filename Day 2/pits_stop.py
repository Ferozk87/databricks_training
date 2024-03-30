# Databricks notebook source
# MAGIC %run "/Workspace/Users/feroz.khan01@marsh.com/Day 2/includes"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/formula1_raw

# COMMAND ----------

df=spark.read.json(f"{location}/pit_stops.json",multiLine=True)

# COMMAND ----------

df_final=df.drop("url").withColumn("ingestion_time",current_timestamp())
df_final.write.mode("overwrite").parquet(f"{output_path}feroz/pitstop")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from parquet.`dbfs:/mnt/cloudthats3/output_formula1/feroz/pitstop`
