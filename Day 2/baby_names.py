# Databricks notebook source
# MAGIC %run "/Workspace/Users/feroz.khan01@marsh.com/Day 2/includes"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/raw/

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/cloudthats3/raw/Baby_Names.csv",header=True,inferSchema= True)

# COMMAND ----------

df.count()

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy("year").count().display()

# COMMAND ----------

print(location)

# COMMAND ----------

df.write.parquet("dbfs:/mnt/cloudthats3/formula1_raw/feroz/baby_names")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/mnt/cloudthats3/formula1_raw/feroz/baby_names`

# COMMAND ----------

df_r=spark.read.parquet("dbfs:/mnt/cloudthats3/formula1_raw/feroz/baby_names")
df_r.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/mnt/cloudthats3/formula1_raw/feroz/baby_names` where year = 2007

# COMMAND ----------

df.write.mode("overwrite").partitionBy("year").parquet("dbfs:/mnt/cloudthats3/formula1_raw/feroz/baby_names_by_year")

# COMMAND ----------

df.write.mode("overwrite").partitionBy("year","sex").parquet("dbfs:/mnt/cloudthats3/formula1_raw/feroz/baby_names_by_year_and_Sex")
