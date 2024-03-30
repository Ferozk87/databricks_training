# Databricks notebook source
# MAGIC %run "/Workspace/Users/feroz.khan01@marsh.com/Day 2/includes"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/raw_json

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/cloudthats3/raw_json/adobe_sample_json.json",multiLine=True)
df.display()

# COMMAND ----------

df.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.display()

# COMMAND ----------

df_final= df.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")\
.withColumn("batters", explode("batters.batter"))\
.withColumn("batters_id",col("batters.id"))\
.withColumn("batters_type",col("batters.type"))\
.drop("batters")


# COMMAND ----------



# COMMAND ----------

df_final.write.mode("overwrite").saveAsTable("feroz.adobe_sample")

# COMMAND ----------

df1 = spark.read.table("feroz.adobe_sample")
df1.display()

# COMMAND ----------

df1.filter("topping_id=5001").show()

# COMMAND ----------

df1.where("topping_id==5001 and batters_type='Chocolate'").show()

# COMMAND ----------

df2=df1.groupBy("batters_type").count().display()


# COMMAND ----------

df2=df1.groupBy("batters_type").count().explain()

# COMMAND ----------

df2=df1

# COMMAND ----------

inner_join=df1.join(df2, on="name", how="inner")
inner_join.display()


# COMMAND ----------

df1.orderBy(col("batters_type").desc()).display()

# COMMAND ----------

df1.sort("batters_type").display()

# COMMAND ----------

df1.orderBy(desc("batters_type"),"topping_id").display()
