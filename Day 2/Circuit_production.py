# Databricks notebook source



# COMMAND ----------

# MAGIC %run "/Workspace/Users/feroz.khan01@marsh.com/Day 2/includes"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/sanly/raw/marsh
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv. `dbfs:/mnt/sanly/raw/marsh/customers.csv`

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/formula1_raw

# COMMAND ----------


df=spark.read.csv(f"{location}/circuits.csv",header=True, inferSchema=True)
df_final=df.drop("url").withColumn("Ingestion_time", current_timestamp())

# COMMAND ----------

df_final.write.mode("overwrite").parquet(f"{output_path}feroz/circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC select constructorId as constructor_id,constructorRef,name,nationality,current_timestamp() as ingestion_date from json.`dbfs:/mnt/cloudthats3/formula1_raw/constructors.json`
