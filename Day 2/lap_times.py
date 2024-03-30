# Databricks notebook source
# MAGIC %run "/Workspace/Users/feroz.khan01@marsh.com/Day 2/includes"

# COMMAND ----------

lap_schema="raceId int, driverID int, lap int, position int, time string, millisecond int"
df=spark.read.schema(lap_schema).csv(f"{location}/lap_times/*",inferSchema=True)

df.write.mode("overwrite").parquet(f"{output_path}feroz/laptimes")

# COMMAND ----------


